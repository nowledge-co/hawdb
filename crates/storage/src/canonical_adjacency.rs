use crate::canonical::{decode_relationship, encode_relationship, CanonicalScanControl};
use crate::graph_descriptor_tree::demand::{
    GraphDescriptorTreeDemandReader, GraphDescriptorTreeReadLimits, GraphDescriptorTreeReadReport,
    GraphDescriptorTreeScanControl,
};
use crate::{
    content_digest, durable_replace_file, AdjacencyDirection, AdjacencyLayout, ContentDigest,
    FileSegmentRangeReader, GraphDescriptorKind, GraphDescriptorPageError,
    GraphDescriptorTreeArtifactMetadata, GraphDescriptorTreeBuildConfig,
    GraphDescriptorTreeBuilder, GraphDescriptorTreeError, GraphDescriptorTreePaths,
    GraphDescriptorTreeRootReader, GraphDescriptorTreeWriteOutput, ManifestGeneration, NodeId,
    PreparedGraphDescriptorTree, RelRecord, SegmentCache, SegmentRangeRead, SegmentReadError,
    SegmentReadRange, StoreId,
};
use skein_core::{RelTypeId, Value};
use skein_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINADJACENCY01";
const BLOCK_HEADER: &[u8; 8] = b"SKNADJ01";
const RUN_HEADER: &[u8; 8] = b"SKNADJR1";
const MANIFEST_HEADER: &str = "SKEIN_CANONICAL_ADJACENCY_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_4144_4a41_4331;
pub const CANONICAL_ADJACENCY_DESCRIPTOR_ARTIFACT_ID: u64 = 0x534b_4744_4144_4a31;
const BLOCK_ID_BASE: u64 = 1 << 63;
const ENTRY_FIXED_BYTES: u64 = 1 + 8 + 4 + 8 + 8 + 4;
const BLOCK_ENTRY_FIXED_BYTES: u64 = 8 + 8 + 4;
const BLOCK_FIXED_BYTES: u64 = 8 + 8 + 8 + 1 + 1 + 8 + 4 + 4;
const DESCRIPTOR_VALUE_MAGIC: &[u8; 8] = b"SKADDS01";
const DESCRIPTOR_VALUE_VERSION: u16 = 1;
const DESCRIPTOR_KEY_BYTES: usize = 29;
const DESCRIPTOR_VALUE_BYTES: usize = 80;

pub fn canonical_adjacency_descriptor_page_file(generation: u64) -> String {
    format!("adjacency-descriptors-{generation}.pages.skein")
}

pub fn canonical_adjacency_descriptor_root_file(generation: u64) -> String {
    format!("adjacency-descriptors-{generation}.root.skein")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalAdjacencyConfig {
    pub memory_budget_bytes: NonZeroU64,
    pub max_spill_bytes: NonZeroU64,
    pub max_spill_runs: NonZeroUsize,
    pub max_merge_fan_in: NonZeroUsize,
    pub target_block_bytes: NonZeroU64,
    pub max_record_bytes: NonZeroU64,
    pub dense_degree_threshold: NonZeroUsize,
}

impl Default for CanonicalAdjacencyConfig {
    fn default() -> Self {
        Self {
            memory_budget_bytes: NonZeroU64::new(32 * 1024 * 1024)
                .expect("default adjacency memory budget is non-zero"),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024)
                .expect("default adjacency spill budget is non-zero"),
            max_spill_runs: NonZeroUsize::new(4_096)
                .expect("default adjacency run budget is non-zero"),
            max_merge_fan_in: NonZeroUsize::new(32)
                .expect("default adjacency merge fan-in is non-zero"),
            target_block_bytes: NonZeroU64::new(1024 * 1024)
                .expect("default adjacency block size is non-zero"),
            max_record_bytes: NonZeroU64::new(16 * 1024 * 1024)
                .expect("default adjacency record size is non-zero"),
            dense_degree_threshold: NonZeroUsize::new(64)
                .expect("default dense degree threshold is non-zero"),
        }
    }
}

#[derive(Debug)]
pub enum CanonicalAdjacencyError {
    Io(std::io::Error),
    Read(SegmentReadError),
    DescriptorTree(GraphDescriptorTreeError),
    Source(String),
    Corrupt(String),
    RecordTooLarge {
        record_bytes: u64,
        max_bytes: u64,
    },
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
    BlockTooLarge {
        block_bytes: u64,
        max_bytes: u64,
    },
}

impl Display for CanonicalAdjacencyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::DescriptorTree(error) => Display::fmt(error, formatter),
            Self::Source(message) | Self::Corrupt(message) => formatter.write_str(message),
            Self::RecordTooLarge {
                record_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency record uses {record_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::MemoryBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency build requires {required_bytes} resident bytes, exceeding {max_bytes}"
            ),
            Self::SpillBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency build requires {required_bytes} spill bytes, exceeding {max_bytes}"
            ),
            Self::SpillRunBudgetExceeded {
                required_runs,
                max_runs,
            } => write!(
                formatter,
                "canonical adjacency build requires {required_runs} spill runs, exceeding {max_runs}"
            ),
            Self::BlockTooLarge {
                block_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency block uses {block_bytes} bytes, exceeding {max_bytes}"
            ),
        }
    }
}

impl Error for CanonicalAdjacencyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::DescriptorTree(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CanonicalAdjacencyError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for CanonicalAdjacencyError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<GraphDescriptorTreeError> for CanonicalAdjacencyError {
    fn from(error: GraphDescriptorTreeError) -> Self {
        Self::DescriptorTree(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAdjacencyBlockDescriptor {
    pub block_id: u64,
    pub direction: AdjacencyDirection,
    pub layout: AdjacencyLayout,
    pub endpoint: NodeId,
    pub rel_type: RelTypeId,
    pub min_neighbor: NodeId,
    pub max_neighbor: NodeId,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub record_count: u32,
}

impl CanonicalAdjacencyBlockDescriptor {
    pub fn descriptor_tree_key(&self) -> [u8; DESCRIPTOR_KEY_BYTES] {
        let mut key = [0u8; DESCRIPTOR_KEY_BYTES];
        key[0] = direction_tag(self.direction);
        key[1..9].copy_from_slice(&self.endpoint.0.to_be_bytes());
        key[9..13].copy_from_slice(&self.rel_type.0.to_be_bytes());
        key[13..21].copy_from_slice(&self.min_neighbor.0.to_be_bytes());
        key[21..29].copy_from_slice(&self.block_id.to_be_bytes());
        key
    }

    pub fn encode_descriptor_tree_value(&self) -> [u8; DESCRIPTOR_VALUE_BYTES] {
        let mut encoded = [0u8; DESCRIPTOR_VALUE_BYTES];
        encoded[..8].copy_from_slice(DESCRIPTOR_VALUE_MAGIC);
        encoded[8..10].copy_from_slice(&DESCRIPTOR_VALUE_VERSION.to_le_bytes());
        encoded[12..20].copy_from_slice(&self.block_id.to_le_bytes());
        encoded[20] = direction_tag(self.direction);
        encoded[21] = layout_tag(self.layout);
        encoded[24..32].copy_from_slice(&self.endpoint.0.to_le_bytes());
        encoded[32..36].copy_from_slice(&self.rel_type.0.to_le_bytes());
        encoded[36..40].copy_from_slice(&self.record_count.to_le_bytes());
        encoded[40..48].copy_from_slice(&self.min_neighbor.0.to_le_bytes());
        encoded[48..56].copy_from_slice(&self.max_neighbor.0.to_le_bytes());
        encoded[56..64].copy_from_slice(&self.offset.to_le_bytes());
        encoded[64..72].copy_from_slice(&self.length.get().to_le_bytes());
        encoded[72..80].copy_from_slice(&self.content_digest.0.to_le_bytes());
        encoded
    }

    pub fn decode_descriptor_tree_entry(
        key: &[u8],
        encoded: &[u8],
    ) -> Result<Self, CanonicalAdjacencyError> {
        if key.len() != DESCRIPTOR_KEY_BYTES
            || encoded.len() != DESCRIPTOR_VALUE_BYTES
            || &encoded[..8] != DESCRIPTOR_VALUE_MAGIC
        {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency descriptor tree entry has an invalid header or length"
                    .to_string(),
            ));
        }
        let version = u16::from_le_bytes(encoded[8..10].try_into().expect("fixed version"));
        let flags = u16::from_le_bytes(encoded[10..12].try_into().expect("fixed flags"));
        if version != DESCRIPTOR_VALUE_VERSION || flags != 0 || encoded[22..24] != [0u8; 2] {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency descriptor tree entry has unsupported version {version}, flags {flags}, or reserved fields"
            )));
        }
        let descriptor = Self {
            block_id: u64::from_le_bytes(encoded[12..20].try_into().expect("fixed block id")),
            direction: direction_from_tag(encoded[20])?,
            layout: layout_from_tag(encoded[21])?,
            endpoint: NodeId(u64::from_le_bytes(
                encoded[24..32].try_into().expect("fixed endpoint"),
            )),
            rel_type: RelTypeId(u32::from_le_bytes(
                encoded[32..36].try_into().expect("fixed relationship type"),
            )),
            record_count: u32::from_le_bytes(
                encoded[36..40].try_into().expect("fixed record count"),
            ),
            min_neighbor: NodeId(u64::from_le_bytes(
                encoded[40..48].try_into().expect("fixed minimum neighbor"),
            )),
            max_neighbor: NodeId(u64::from_le_bytes(
                encoded[48..56].try_into().expect("fixed maximum neighbor"),
            )),
            offset: u64::from_le_bytes(encoded[56..64].try_into().expect("fixed offset")),
            length: NonZeroU64::new(u64::from_le_bytes(
                encoded[64..72].try_into().expect("fixed length"),
            ))
            .ok_or_else(|| {
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency descriptor tree block length is zero".to_string(),
                )
            })?,
            content_digest: ContentDigest(u64::from_le_bytes(
                encoded[72..80].try_into().expect("fixed digest"),
            )),
        };
        if descriptor.block_id < BLOCK_ID_BASE
            || descriptor.record_count == 0
            || descriptor.min_neighbor > descriptor.max_neighbor
            || descriptor.descriptor_tree_key().as_slice() != key
        {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency descriptor tree key and value are inconsistent".to_string(),
            ));
        }
        descriptor
            .offset
            .checked_add(descriptor.length.get())
            .ok_or_else(|| {
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency descriptor tree block range overflows u64".to_string(),
                )
            })?;
        Ok(descriptor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAdjacencyManifest {
    pub generation: ManifestGeneration,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub artifact_sha256: Sha256Digest,
    pub relationship_count: u64,
    pub entry_count: u64,
    pub blocks: Vec<CanonicalAdjacencyBlockDescriptor>,
}

impl CanonicalAdjacencyManifest {
    pub fn validate(&self) -> Result<(), CanonicalAdjacencyError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency manifest has an unsupported artifact id".to_string(),
            ));
        }
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8 {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact is shorter than its header".to_string(),
            ));
        }
        if self.entry_count != self.relationship_count.saturating_mul(2) {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency entry count is not twice its relationship count".to_string(),
            ));
        }
        let mut previous_end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut previous_key = None;
        let mut total_entries = 0u64;
        for block in &self.blocks {
            if block.block_id < BLOCK_ID_BASE || block.record_count == 0 {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency block {} has invalid identity or cardinality",
                    block.block_id
                )));
            }
            if block.min_neighbor > block.max_neighbor || block.offset < previous_end {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency block {} has invalid bounds",
                    block.block_id
                )));
            }
            let key = descriptor_key(block);
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency blocks are not strictly ordered".to_string(),
                ));
            }
            previous_key = Some(key);
            previous_end = block
                .offset
                .checked_add(block.length.get())
                .ok_or_else(|| {
                    CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency block range overflows u64".to_string(),
                    )
                })?;
            if previous_end > self.artifact_len {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency block exceeds its artifact".to_string(),
                ));
            }
            total_entries = total_entries.saturating_add(u64::from(block.record_count));
        }
        if total_entries != self.entry_count {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency manifest counts {total_entries} entries but declares {}",
                self.entry_count
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, CanonicalAdjacencyError> {
        self.validate()?;
        let mut output = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nrelationship_count\t{}\nentry_count\t{}\n",
            self.generation.0,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.artifact_sha256,
            self.relationship_count,
            self.entry_count
        );
        for block in &self.blocks {
            output.push_str(&format!(
                "block\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                block.block_id,
                direction_tag(block.direction),
                layout_tag(block.layout),
                block.endpoint.0,
                block.rel_type.0,
                block.min_neighbor.0,
                block.max_neighbor.0,
                block.offset,
                block.length.get(),
                block.content_digest.0,
                block.record_count
            ));
        }
        Ok(output)
    }

    pub fn decode(encoded: &str) -> Result<Self, CanonicalAdjacencyError> {
        let mut generation = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut artifact_sha256 = None;
        let mut relationship_count = None;
        let mut entry_count = None;
        let mut blocks = Vec::new();
        for (line_number, line) in encoded.lines().enumerate() {
            if line_number == 0 {
                if line != MANIFEST_HEADER {
                    return Err(CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency manifest has an invalid header".to_string(),
                    ));
                }
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["generation", value] => generation = Some(parse_u64(value, "generation")?),
                ["artifact_id", value] => artifact_id = Some(parse_u64(value, "artifact id")?),
                ["artifact_len", value] => {
                    artifact_len = Some(parse_u64(value, "artifact length")?)
                }
                ["artifact_digest", value] => {
                    artifact_digest = Some(parse_u64(value, "artifact digest")?)
                }
                ["artifact_sha256", value] => {
                    artifact_sha256 = Some(value.parse().map_err(|error| {
                        CanonicalAdjacencyError::Corrupt(format!(
                            "invalid artifact SHA-256 digest: {error}"
                        ))
                    })?)
                }
                ["relationship_count", value] => {
                    relationship_count = Some(parse_u64(value, "relationship count")?)
                }
                ["entry_count", value] => entry_count = Some(parse_u64(value, "entry count")?),
                ["block", block_id, direction, layout, endpoint, rel_type, min_neighbor, max_neighbor, offset, length, digest, record_count] => {
                    blocks.push(CanonicalAdjacencyBlockDescriptor {
                        block_id: parse_u64(block_id, "block id")?,
                        direction: direction_from_tag(parse_u64(direction, "direction")? as u8)?,
                        layout: layout_from_tag(parse_u64(layout, "layout")? as u8)?,
                        endpoint: NodeId(parse_u64(endpoint, "endpoint")?),
                        rel_type: RelTypeId(parse_u32(rel_type, "relationship type")?),
                        min_neighbor: NodeId(parse_u64(min_neighbor, "minimum neighbor")?),
                        max_neighbor: NodeId(parse_u64(max_neighbor, "maximum neighbor")?),
                        offset: parse_u64(offset, "block offset")?,
                        length: NonZeroU64::new(parse_u64(length, "block length")?).ok_or_else(
                            || {
                                CanonicalAdjacencyError::Corrupt(
                                    "canonical adjacency block length is zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "block digest")?),
                        record_count: parse_u32(record_count, "block record count")?,
                    })
                }
                _ => {
                    return Err(CanonicalAdjacencyError::Corrupt(format!(
                        "invalid canonical adjacency manifest line: {line}"
                    )));
                }
            }
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            artifact_sha256: required(artifact_sha256, "artifact SHA-256 digest")?,
            relationship_count: required(relationship_count, "relationship count")?,
            entry_count: required(entry_count, "entry count")?,
            blocks,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalAdjacencyBuildReport {
    pub relationship_count: u64,
    pub entry_count: u64,
    pub sparse_block_count: u64,
    pub dense_block_count: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_resident_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct CanonicalAdjacencyWriteOutput {
    pub generation: ManifestGeneration,
    pub artifact: CanonicalAdjacencyArtifactMetadata,
    pub resident_manifest: Option<CanonicalAdjacencyManifest>,
    pub report: CanonicalAdjacencyBuildReport,
    pub descriptor_tree: Option<GraphDescriptorTreeWriteOutput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalAdjacencyArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_crc32c: u64,
    pub encoded_sha256: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalAdjacencyGenerationArtifacts {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub relationship_count: u64,
    pub entry_count: u64,
    pub adjacency_artifact: CanonicalAdjacencyArtifactMetadata,
    pub descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata,
}

impl CanonicalAdjacencyWriteOutput {
    pub fn generation_artifacts(&self) -> Option<CanonicalAdjacencyGenerationArtifacts> {
        let descriptor_tree = self.descriptor_tree.as_ref()?;
        Some(CanonicalAdjacencyGenerationArtifacts {
            generation: self.generation.0,
            source_commit_epoch: descriptor_tree.root.source_commit_epoch,
            relationship_count: self.report.relationship_count,
            entry_count: self.report.entry_count,
            adjacency_artifact: self.artifact,
            descriptor_root_artifact: descriptor_tree.root_artifact,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalAdjacencyEntry {
    Inline(RelRecord),
    CanonicalReference { relationship_id: crate::RelId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey {
    direction: u8,
    endpoint: u64,
    rel_type: u32,
    neighbor: u64,
    rel_id: u64,
}

#[derive(Debug)]
struct EncodedEntry {
    key: EntryKey,
    payload: Vec<u8>,
}

impl EncodedEntry {
    fn run_encoded_len(&self) -> u64 {
        ENTRY_FIXED_BYTES.saturating_add(self.payload.len() as u64)
    }

    fn block_encoded_len(&self) -> u64 {
        BLOCK_ENTRY_FIXED_BYTES.saturating_add(self.payload.len() as u64)
    }

    fn resident_bytes(&self) -> u64 {
        self.run_encoded_len()
            .saturating_add(std::mem::size_of::<Self>() as u64)
    }
}

pub struct CanonicalAdjacencyWriter {
    config: CanonicalAdjacencyConfig,
}

impl CanonicalAdjacencyWriter {
    pub const fn new(config: CanonicalAdjacencyConfig) -> Self {
        Self { config }
    }

    pub fn write_fallible<R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        relationships: R,
    ) -> Result<CanonicalAdjacencyWriteOutput, CanonicalAdjacencyError>
    where
        R: IntoIterator<Item = Result<RelRecord, CanonicalAdjacencyError>>,
    {
        self.write_fallible_internal(path, generation, None, relationships)
    }

    pub fn write_fallible_with_descriptor_tree<R>(
        &self,
        path: &Path,
        descriptor_paths: GraphDescriptorTreePaths,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        descriptor_config: GraphDescriptorTreeBuildConfig,
        relationships: R,
    ) -> Result<CanonicalAdjacencyWriteOutput, CanonicalAdjacencyError>
    where
        R: IntoIterator<Item = Result<RelRecord, CanonicalAdjacencyError>>,
    {
        self.write_fallible_internal(
            path,
            generation,
            Some((descriptor_paths, source_commit_epoch, descriptor_config)),
            relationships,
        )
    }

    fn write_fallible_internal<R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        descriptor_tree: Option<(
            GraphDescriptorTreePaths,
            u64,
            GraphDescriptorTreeBuildConfig,
        )>,
        relationships: R,
    ) -> Result<CanonicalAdjacencyWriteOutput, CanonicalAdjacencyError>
    where
        R: IntoIterator<Item = Result<RelRecord, CanonicalAdjacencyError>>,
    {
        reject_existing_immutable_artifact(path, "canonical adjacency data")?;
        if let Some((paths, _, _)) = &descriptor_tree {
            reject_existing_immutable_artifact(
                &paths.page_artifact,
                "canonical adjacency descriptor pages",
            )?;
            reject_existing_immutable_artifact(
                &paths.root_manifest,
                "canonical adjacency descriptor root",
            )?;
        }
        let mut runs = SpillRuns::new(path, generation, self.config);
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut relationship_count = 0u64;
        let mut peak_resident_bytes = 0u64;
        for relationship in relationships {
            let relationship = relationship?;
            let payload = if estimated_relationship_payload_bytes(&relationship)
                <= self.config.max_record_bytes.get()
            {
                let payload = encode_relationship(&relationship)
                    .map_err(|error| CanonicalAdjacencyError::Source(error.to_string()))?;
                if payload.len() as u64 <= self.config.max_record_bytes.get() {
                    payload
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let outgoing = EncodedEntry {
                key: EntryKey {
                    direction: direction_tag(AdjacencyDirection::Outgoing),
                    endpoint: relationship.source.0,
                    rel_type: relationship.rel_type.0,
                    neighbor: relationship.target.0,
                    rel_id: relationship.id.0,
                },
                payload: payload.clone(),
            };
            let incoming = EncodedEntry {
                key: EntryKey {
                    direction: direction_tag(AdjacencyDirection::Incoming),
                    endpoint: relationship.target.0,
                    rel_type: relationship.rel_type.0,
                    neighbor: relationship.source.0,
                    rel_id: relationship.id.0,
                },
                payload,
            };
            for entry in [outgoing, incoming] {
                let entry_bytes = entry.resident_bytes();
                if entry_bytes > self.config.memory_budget_bytes.get() {
                    return Err(CanonicalAdjacencyError::MemoryBudgetExceeded {
                        required_bytes: entry_bytes,
                        max_bytes: self.config.memory_budget_bytes.get(),
                    });
                }
                if !chunk.is_empty()
                    && chunk_bytes.saturating_add(entry_bytes)
                        > self.config.memory_budget_bytes.get()
                {
                    runs.spill(&mut chunk)?;
                    chunk_bytes = 0;
                }
                chunk_bytes = chunk_bytes.saturating_add(entry_bytes);
                peak_resident_bytes = peak_resident_bytes.max(chunk_bytes);
                chunk.push(entry);
            }
            relationship_count = relationship_count.checked_add(1).ok_or_else(|| {
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency relationship count overflow".to_string(),
                )
            })?;
        }
        if !chunk.is_empty() {
            runs.spill(&mut chunk)?;
        }
        runs.compact()?;

        let tmp_path = path.with_extension("skein.tmp");
        let result = self.merge_runs(
            &tmp_path,
            generation,
            relationship_count,
            peak_resident_bytes,
            &runs,
            descriptor_tree,
        );
        let (mut output, prepared_descriptor_tree) = match result {
            Ok(output) => output,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        durable_replace_file(&tmp_path, path)?;
        if let Some(prepared) = prepared_descriptor_tree {
            output.descriptor_tree = Some(prepared.publish()?);
        }
        Ok(output)
    }

    fn merge_runs(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        relationship_count: u64,
        peak_resident_bytes: u64,
        runs: &SpillRuns,
        descriptor_tree: Option<(
            GraphDescriptorTreePaths,
            u64,
            GraphDescriptorTreeBuildConfig,
        )>,
    ) -> Result<
        (
            CanonicalAdjacencyWriteOutput,
            Option<PreparedGraphDescriptorTree>,
        ),
        CanonicalAdjacencyError,
    > {
        let file = File::create(path)?;
        let descriptor_tree = descriptor_tree
            .map(|(paths, source_commit_epoch, config)| {
                GraphDescriptorTreeBuilder::create(
                    paths,
                    GraphDescriptorKind::CanonicalAdjacency,
                    generation.0,
                    source_commit_epoch,
                    CANONICAL_ADJACENCY_DESCRIPTOR_ARTIFACT_ID,
                    config,
                )
            })
            .transpose()?;
        let mut artifact = ArtifactBuilder::new(file, generation, self.config, descriptor_tree)?;
        let mut readers = runs
            .paths
            .iter()
            .map(|path| RunReader::open(path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            let key = reader.next_key()?;
            if let Some(key) = key {
                heap.push(Reverse((key, index)));
            }
            current.push(key);
        }
        let mut previous_key = None;
        while let Some(Reverse((key, run_index))) = heap.pop() {
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency spill merge encountered duplicate or unordered keys"
                        .to_string(),
                ));
            }
            if current[run_index] != Some(key) {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency spill heap does not match its reader".to_string(),
                ));
            }
            let entry = readers[run_index].take_entry()?;
            artifact.push(entry)?;
            previous_key = Some(key);
            current[run_index] = readers[run_index].next_key()?;
            if let Some(next) = current[run_index] {
                heap.push(Reverse((next, run_index)));
            }
        }
        let FinishedArtifact {
            artifact,
            entry_count,
            resident_manifest,
            sparse_block_count,
            dense_block_count,
            descriptor_tree: prepared_descriptor_tree,
        } = artifact.finish(relationship_count)?;
        Ok((
            CanonicalAdjacencyWriteOutput {
                generation,
                artifact,
                resident_manifest,
                report: CanonicalAdjacencyBuildReport {
                    relationship_count,
                    entry_count,
                    sparse_block_count,
                    dense_block_count,
                    spill_run_count: runs.next_run_sequence,
                    spill_bytes: runs.spill_bytes,
                    peak_resident_bytes,
                },
                descriptor_tree: None,
            },
            prepared_descriptor_tree,
        ))
    }
}

struct SpillRuns {
    prefix: PathBuf,
    generation: ManifestGeneration,
    config: CanonicalAdjacencyConfig,
    paths: Vec<PathBuf>,
    spill_bytes: u64,
    next_run_sequence: usize,
}

impl SpillRuns {
    fn new(path: &Path, generation: ManifestGeneration, config: CanonicalAdjacencyConfig) -> Self {
        Self {
            prefix: path.to_path_buf(),
            generation,
            config,
            paths: Vec::new(),
            spill_bytes: 0,
            next_run_sequence: 0,
        }
    }

    fn spill(&mut self, entries: &mut Vec<EncodedEntry>) -> Result<(), CanonicalAdjacencyError> {
        let required_runs = self.paths.len().saturating_add(1);
        if required_runs > self.config.max_spill_runs.get() {
            return Err(CanonicalAdjacencyError::SpillRunBudgetExceeded {
                required_runs,
                max_runs: self.config.max_spill_runs.get(),
            });
        }
        entries.sort_unstable_by_key(|entry| entry.key);
        let run_bytes = RUN_HEADER.len() as u64
            + entries
                .iter()
                .map(EncodedEntry::run_encoded_len)
                .fold(0u64, u64::saturating_add);
        let required_bytes = self.spill_bytes.saturating_add(run_bytes);
        if required_bytes > self.config.max_spill_bytes.get() {
            return Err(CanonicalAdjacencyError::SpillBudgetExceeded {
                required_bytes,
                max_bytes: self.config.max_spill_bytes.get(),
            });
        }
        let path = self.next_path();
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(RUN_HEADER)?;
        for entry in entries.iter() {
            write_entry(&mut writer, entry)?;
        }
        writer.flush()?;
        self.paths.push(path);
        self.spill_bytes = required_bytes;
        entries.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<(), CanonicalAdjacencyError> {
        let fan_in = self.config.max_merge_fan_in.get();
        if fan_in < 2 {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > fan_in {
            let old_paths = std::mem::take(&mut self.paths);
            let mut merged_paths = Vec::with_capacity(old_paths.len().div_ceil(fan_in));
            for group in old_paths.chunks(fan_in) {
                let path = self.next_path();
                let bytes = match merge_run_group(group, &path) {
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
                    return Err(CanonicalAdjacencyError::SpillBudgetExceeded {
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

    fn next_path(&mut self) -> PathBuf {
        let sequence = self.next_run_sequence;
        self.next_run_sequence = self.next_run_sequence.saturating_add(1);
        self.prefix.with_file_name(format!(
            ".adjacency.{}.run.{sequence}.tmp",
            self.generation.0
        ))
    }
}

impl Drop for SpillRuns {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

struct RunReader {
    reader: BufReader<File>,
    pending: Option<(EntryKey, usize)>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self, CanonicalAdjacencyError> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill run has an invalid header".to_string(),
            ));
        }
        Ok(Self {
            reader,
            pending: None,
        })
    }

    fn next_key(&mut self) -> Result<Option<EntryKey>, CanonicalAdjacencyError> {
        if self.pending.is_some() {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill reader advanced before consuming its payload"
                    .to_string(),
            ));
        }
        let mut direction = [0u8; 1];
        match self.reader.read(&mut direction)? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one byte read buffer"),
        }
        let endpoint = read_u64(&mut self.reader)?;
        let rel_type = read_u32(&mut self.reader)?;
        let neighbor = read_u64(&mut self.reader)?;
        let rel_id = read_u64(&mut self.reader)?;
        let payload_len = read_u32(&mut self.reader)? as usize;
        let key = EntryKey {
            direction: direction[0],
            endpoint,
            rel_type,
            neighbor,
            rel_id,
        };
        self.pending = Some((key, payload_len));
        Ok(Some(key))
    }

    fn take_entry(&mut self) -> Result<EncodedEntry, CanonicalAdjacencyError> {
        let (key, payload_len) = self.pending.take().ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill reader has no pending entry".to_string(),
            )
        })?;
        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload)?;
        Ok(EncodedEntry { key, payload })
    }
}

fn merge_run_group(
    sources: &[PathBuf],
    destination: &Path,
) -> Result<u64, CanonicalAdjacencyError> {
    let mut readers = sources
        .iter()
        .map(|path| RunReader::open(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        let key = reader.next_key()?;
        if let Some(key) = key {
            heap.push(Reverse((key, index)));
        }
        current.push(key);
    }
    let mut writer = BufWriter::new(File::create(destination)?);
    writer.write_all(RUN_HEADER)?;
    let mut bytes = RUN_HEADER.len() as u64;
    let mut previous_key = None;
    while let Some(Reverse((key, run_index))) = heap.pop() {
        if current[run_index] != Some(key) || previous_key.is_some_and(|previous| previous >= key) {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill compaction encountered unordered keys".to_string(),
            ));
        }
        let entry = readers[run_index].take_entry()?;
        write_entry(&mut writer, &entry)?;
        bytes = bytes.saturating_add(entry.run_encoded_len());
        previous_key = Some(key);
        current[run_index] = readers[run_index].next_key()?;
        if let Some(next) = current[run_index] {
            heap.push(Reverse((next, run_index)));
        }
    }
    writer.flush()?;
    Ok(bytes)
}

struct ArtifactBuilder {
    writer: BufWriter<File>,
    digest: IntegrityHasher,
    generation: ManifestGeneration,
    config: CanonicalAdjacencyConfig,
    artifact_len: u64,
    resident_blocks: Option<Vec<CanonicalAdjacencyBlockDescriptor>>,
    group: Option<PendingGroup>,
    next_block_id: u64,
    entry_count: u64,
    sparse_block_count: u64,
    dense_block_count: u64,
    descriptor_tree: Option<GraphDescriptorTreeBuilder>,
}

struct FinishedArtifact {
    artifact: CanonicalAdjacencyArtifactMetadata,
    entry_count: u64,
    resident_manifest: Option<CanonicalAdjacencyManifest>,
    sparse_block_count: u64,
    dense_block_count: u64,
    descriptor_tree: Option<PreparedGraphDescriptorTree>,
}

impl ArtifactBuilder {
    fn new(
        file: File,
        generation: ManifestGeneration,
        config: CanonicalAdjacencyConfig,
        descriptor_tree: Option<GraphDescriptorTreeBuilder>,
    ) -> Result<Self, CanonicalAdjacencyError> {
        let mut writer = BufWriter::new(file);
        let mut digest = IntegrityHasher::new();
        write_hashed(&mut writer, &mut digest, ARTIFACT_HEADER)?;
        write_hashed(&mut writer, &mut digest, &generation.0.to_le_bytes())?;
        let collect_resident_manifest = descriptor_tree.is_none();
        Ok(Self {
            writer,
            digest,
            generation,
            config,
            artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
            resident_blocks: collect_resident_manifest.then(Vec::new),
            group: None,
            next_block_id: BLOCK_ID_BASE,
            entry_count: 0,
            sparse_block_count: 0,
            dense_block_count: 0,
            descriptor_tree,
        })
    }

    fn push(&mut self, entry: EncodedEntry) -> Result<(), CanonicalAdjacencyError> {
        let key = GroupKey::from_entry(&entry)?;
        if self.group.as_ref().is_some_and(|group| group.key != key) {
            self.finish_group()?;
        }
        if self.group.is_none() {
            self.group = Some(PendingGroup::new(key));
        }
        let threshold = self.config.dense_degree_threshold.get();
        let mut group = self.group.take().expect("adjacency group was initialized");
        if !group.dense {
            let entry_bytes = entry.resident_bytes();
            let exceeds_group_budget = !group.buffer.is_empty()
                && group.buffer_bytes.saturating_add(entry_bytes)
                    > self.config.memory_budget_bytes.get();
            if exceeds_group_budget {
                group.dense = true;
                for buffered in std::mem::take(&mut group.buffer) {
                    self.push_dense_entry(&mut group, buffered)?;
                }
                group.buffer_bytes = 0;
                self.push_dense_entry(&mut group, entry)?;
            } else {
                group.buffer_bytes = group.buffer_bytes.saturating_add(entry_bytes);
                group.buffer.push(entry);
                if group.buffer.len() >= threshold {
                    group.dense = true;
                    for buffered in std::mem::take(&mut group.buffer) {
                        self.push_dense_entry(&mut group, buffered)?;
                    }
                    group.buffer_bytes = 0;
                }
            }
        } else {
            self.push_dense_entry(&mut group, entry)?;
        }
        self.group = Some(group);
        self.entry_count = self.entry_count.checked_add(1).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt("canonical adjacency entry count overflow".to_string())
        })?;
        Ok(())
    }

    fn push_dense_entry(
        &mut self,
        group: &mut PendingGroup,
        entry: EncodedEntry,
    ) -> Result<(), CanonicalAdjacencyError> {
        let prospective = group
            .block_bytes
            .saturating_add(entry.block_encoded_len())
            .saturating_add(BLOCK_FIXED_BYTES);
        let target = self
            .config
            .target_block_bytes
            .get()
            .min(self.config.memory_budget_bytes.get());
        if !group.block.is_empty() && prospective > target {
            self.flush_group_block(group, AdjacencyLayout::Dense)?;
        }
        group.block_bytes = group.block_bytes.saturating_add(entry.block_encoded_len());
        group.block.push(entry);
        Ok(())
    }

    fn finish_group(&mut self) -> Result<(), CanonicalAdjacencyError> {
        let Some(mut group) = self.group.take() else {
            return Ok(());
        };
        if group.dense {
            self.flush_group_block(&mut group, AdjacencyLayout::Dense)?;
        } else if !group.buffer.is_empty() {
            group.block_bytes = group
                .buffer
                .iter()
                .map(EncodedEntry::block_encoded_len)
                .fold(0u64, u64::saturating_add);
            group.block = std::mem::take(&mut group.buffer);
            group.buffer_bytes = 0;
            self.flush_group_block(&mut group, AdjacencyLayout::Sparse)?;
        }
        Ok(())
    }

    fn flush_group_block(
        &mut self,
        group: &mut PendingGroup,
        layout: AdjacencyLayout,
    ) -> Result<(), CanonicalAdjacencyError> {
        if group.block.is_empty() {
            return Ok(());
        }
        let record_count = u32::try_from(group.block.len()).map_err(|_| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency block record count exceeds u32".to_string(),
            )
        })?;
        let block_bytes = BLOCK_FIXED_BYTES.saturating_add(group.block_bytes);
        let hard_max = self.config.target_block_bytes.get().max(
            self.config
                .max_record_bytes
                .get()
                .saturating_add(BLOCK_FIXED_BYTES),
        );
        if block_bytes > hard_max {
            return Err(CanonicalAdjacencyError::BlockTooLarge {
                block_bytes,
                max_bytes: hard_max,
            });
        }
        let min_neighbor = NodeId(
            group
                .block
                .first()
                .expect("flushed adjacency block is non-empty")
                .key
                .neighbor,
        );
        let max_neighbor = NodeId(
            group
                .block
                .last()
                .expect("flushed adjacency block is non-empty")
                .key
                .neighbor,
        );
        let length = NonZeroU64::new(block_bytes).expect("canonical adjacency block is non-empty");
        let mut block_digest = Crc32cHasher::new();
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            BLOCK_HEADER,
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &self.generation.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &self.next_block_id.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &[direction_tag(group.key.direction), layout_tag(layout)],
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &group.key.endpoint.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &group.key.rel_type.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &record_count.to_le_bytes(),
        )?;
        for entry in &group.block {
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &entry.key.neighbor.to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &entry.key.rel_id.to_le_bytes(),
            )?;
            let payload_len = u32::try_from(entry.payload.len()).map_err(|_| {
                CanonicalAdjacencyError::RecordTooLarge {
                    record_bytes: entry.payload.len() as u64,
                    max_bytes: u64::from(u32::MAX),
                }
            })?;
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &payload_len.to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &entry.payload,
            )?;
        }
        let descriptor = CanonicalAdjacencyBlockDescriptor {
            block_id: self.next_block_id,
            direction: group.key.direction,
            layout,
            endpoint: group.key.endpoint,
            rel_type: group.key.rel_type,
            min_neighbor,
            max_neighbor,
            offset: self.artifact_len,
            length,
            content_digest: ContentDigest(block_digest.finish()),
            record_count,
        };
        self.artifact_len = self.artifact_len.checked_add(length.get()).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact length overflow".to_string(),
            )
        })?;
        self.next_block_id = self.next_block_id.checked_add(1).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt("canonical adjacency block id overflow".to_string())
        })?;
        match layout {
            AdjacencyLayout::Sparse => {
                self.sparse_block_count =
                    self.sparse_block_count.checked_add(1).ok_or_else(|| {
                        CanonicalAdjacencyError::Corrupt(
                            "canonical adjacency sparse block count overflow".to_string(),
                        )
                    })?;
            }
            AdjacencyLayout::Dense => {
                self.dense_block_count =
                    self.dense_block_count.checked_add(1).ok_or_else(|| {
                        CanonicalAdjacencyError::Corrupt(
                            "canonical adjacency dense block count overflow".to_string(),
                        )
                    })?;
            }
        }
        if let Some(descriptor_tree) = &mut self.descriptor_tree {
            descriptor_tree.push(
                descriptor.descriptor_tree_key().to_vec(),
                descriptor.encode_descriptor_tree_value().to_vec(),
            )?;
        }
        if let Some(blocks) = &mut self.resident_blocks {
            blocks.push(descriptor);
        }
        group.block.clear();
        group.block_bytes = 0;
        Ok(())
    }

    fn finish(
        mut self,
        relationship_count: u64,
    ) -> Result<FinishedArtifact, CanonicalAdjacencyError> {
        self.finish_group()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let artifact_integrity = self.digest.finish();
        let artifact = CanonicalAdjacencyArtifactMetadata {
            encoded_len: self.artifact_len,
            encoded_crc32c: artifact_integrity.crc32c.as_u64(),
            encoded_sha256: artifact_integrity.sha256,
        };
        let resident_manifest =
            self.resident_blocks
                .take()
                .map(|blocks| CanonicalAdjacencyManifest {
                    generation: self.generation,
                    artifact_id: ARTIFACT_ID,
                    artifact_len: artifact.encoded_len,
                    artifact_digest: ContentDigest(artifact.encoded_crc32c),
                    artifact_sha256: artifact.encoded_sha256,
                    relationship_count,
                    entry_count: self.entry_count,
                    blocks,
                });
        if let Some(manifest) = &resident_manifest {
            manifest.validate()?;
        }
        let expected_entries = relationship_count.checked_mul(2).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency relationship count overflow".to_string(),
            )
        })?;
        if self.entry_count != expected_entries {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency wrote {} entries for {relationship_count} relationships",
                self.entry_count
            )));
        }
        let descriptor_tree = self
            .descriptor_tree
            .take()
            .map(GraphDescriptorTreeBuilder::finish)
            .transpose()?;
        Ok(FinishedArtifact {
            artifact,
            entry_count: self.entry_count,
            resident_manifest,
            sparse_block_count: self.sparse_block_count,
            dense_block_count: self.dense_block_count,
            descriptor_tree,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GroupKey {
    direction: AdjacencyDirection,
    endpoint: NodeId,
    rel_type: RelTypeId,
}

impl GroupKey {
    fn from_entry(entry: &EncodedEntry) -> Result<Self, CanonicalAdjacencyError> {
        Ok(Self {
            direction: direction_from_tag(entry.key.direction)?,
            endpoint: NodeId(entry.key.endpoint),
            rel_type: RelTypeId(entry.key.rel_type),
        })
    }
}

struct PendingGroup {
    key: GroupKey,
    dense: bool,
    buffer: Vec<EncodedEntry>,
    buffer_bytes: u64,
    block: Vec<EncodedEntry>,
    block_bytes: u64,
}

impl PendingGroup {
    fn new(key: GroupKey) -> Self {
        Self {
            key,
            dense: false,
            buffer: Vec::new(),
            buffer_bytes: 0,
            block: Vec::new(),
            block_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalAdjacencyReadReport {
    pub generation: u64,
    pub descriptor_pages_visited: u64,
    pub descriptor_page_bytes_decoded: u64,
    pub descriptor_storage_bytes_read: u64,
    pub descriptors_examined: u64,
    pub descriptor_cache_hits: u64,
    pub descriptor_cache_misses: u64,
    pub descriptor_cache_admission_rejections: u64,
    pub blocks_considered: u64,
    pub blocks_read: u64,
    pub bytes_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub records_decoded: u64,
    pub sparse_blocks_read: u64,
    pub dense_blocks_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalAdjacencyScrubReport {
    pub descriptor_pages_checked: u64,
    pub descriptors_checked: u64,
    pub descriptor_bytes_checked: u64,
    pub adjacency_blocks_checked: u64,
    pub adjacency_records_checked: u64,
    pub adjacency_bytes_hashed: u64,
}

#[derive(Debug, Clone)]
enum CanonicalAdjacencyDescriptorBackend {
    Resident(Arc<CanonicalAdjacencyManifest>),
    Demand(GraphDescriptorTreeDemandReader),
}

#[derive(Debug, Clone)]
pub struct CanonicalAdjacencyReader {
    path: PathBuf,
    generation: ManifestGeneration,
    artifact_len: u64,
    artifact_digest: ContentDigest,
    artifact_sha256: Sha256Digest,
    relationship_count: u64,
    entry_count: u64,
    descriptors: CanonicalAdjacencyDescriptorBackend,
    range_reader: FileSegmentRangeReader,
    max_block_bytes: NonZeroU64,
    poisoned: Arc<AtomicBool>,
}

impl CanonicalAdjacencyReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: CanonicalAdjacencyManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, CanonicalAdjacencyError> {
        manifest.validate()?;
        let path = path.into();
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        for block in &manifest.blocks {
            if block.length.get() > max_block_bytes.get() {
                return Err(CanonicalAdjacencyError::BlockTooLarge {
                    block_bytes: block.length.get(),
                    max_bytes: max_block_bytes.get(),
                });
            }
        }
        let mut range_reader =
            FileSegmentRangeReader::new().with_cache(cache, store_id, manifest.generation);
        range_reader.register(manifest.artifact_id, path.clone());
        Ok(Self {
            path,
            generation: manifest.generation,
            artifact_len: manifest.artifact_len,
            artifact_digest: manifest.artifact_digest,
            artifact_sha256: manifest.artifact_sha256,
            relationship_count: manifest.relationship_count,
            entry_count: manifest.entry_count,
            descriptors: CanonicalAdjacencyDescriptorBackend::Resident(Arc::new(manifest)),
            range_reader,
            max_block_bytes,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn open_demand_paged(
        path: impl Into<PathBuf>,
        binding: CanonicalAdjacencyGenerationArtifacts,
        root_reader: GraphDescriptorTreeRootReader,
        descriptor_config: GraphDescriptorTreeBuildConfig,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, CanonicalAdjacencyError> {
        if binding.generation == 0
            || binding.adjacency_artifact.encoded_len < ARTIFACT_HEADER.len() as u64 + 8
            || binding.entry_count
                != binding.relationship_count.checked_mul(2).ok_or_else(|| {
                    CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency relationship count overflow".to_string(),
                    )
                })?
        {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency generation binding has invalid counts or artifact length"
                    .to_string(),
            ));
        }
        let root = root_reader.root();
        if root.kind != GraphDescriptorKind::CanonicalAdjacency
            || root.generation != binding.generation
            || root.source_commit_epoch != binding.source_commit_epoch
            || root.page_artifact_id != CANONICAL_ADJACENCY_DESCRIPTOR_ARTIFACT_ID
            || root.root.is_some() != (binding.entry_count != 0)
        {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency descriptor root does not match its generation binding"
                    .to_string(),
            ));
        }
        let path = path.into();
        validate_artifact_header(
            &path,
            binding.generation,
            binding.adjacency_artifact.encoded_len,
        )?;
        let demand = GraphDescriptorTreeDemandReader::open(
            root_reader,
            descriptor_config,
            Arc::clone(&cache),
            store_id,
        )?;
        let mut range_reader = FileSegmentRangeReader::new().with_cache(
            cache,
            store_id,
            ManifestGeneration(binding.generation),
        );
        range_reader.register(ARTIFACT_ID, path.clone());
        Ok(Self {
            path,
            generation: ManifestGeneration(binding.generation),
            artifact_len: binding.adjacency_artifact.encoded_len,
            artifact_digest: ContentDigest(binding.adjacency_artifact.encoded_crc32c),
            artifact_sha256: binding.adjacency_artifact.encoded_sha256,
            relationship_count: binding.relationship_count,
            entry_count: binding.entry_count,
            descriptors: CanonicalAdjacencyDescriptorBackend::Demand(demand),
            range_reader,
            max_block_bytes,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn generation(&self) -> ManifestGeneration {
        self.generation
    }

    pub const fn artifact_len(&self) -> u64 {
        self.artifact_len
    }

    pub const fn relationship_count(&self) -> u64 {
        self.relationship_count
    }

    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub fn estimate_endpoint_entries(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: Option<RelTypeId>,
    ) -> Result<u64, CanonicalAdjacencyError> {
        self.ensure_healthy()?;
        match &self.descriptors {
            CanonicalAdjacencyDescriptorBackend::Resident(manifest) => {
                let direction = direction_tag(direction);
                let start = manifest.blocks.partition_point(|block| {
                    (direction_tag(block.direction), block.endpoint.0) < (direction, endpoint.0)
                });
                let end = manifest.blocks.partition_point(|block| {
                    (direction_tag(block.direction), block.endpoint.0) <= (direction, endpoint.0)
                });
                Ok(manifest.blocks[start..end]
                    .iter()
                    .filter(|block| rel_type.is_none_or(|expected| block.rel_type == expected))
                    .map(|block| u64::from(block.record_count))
                    .sum())
            }
            CanonicalAdjacencyDescriptorBackend::Demand(demand) => {
                let prefix = descriptor_prefix(endpoint, direction, rel_type);
                let mut count = 0u64;
                let mut decode_error = None;
                let result = demand.scan_prefix(
                    &prefix,
                    GraphDescriptorTreeReadLimits::default(),
                    |key, value| {
                        match CanonicalAdjacencyBlockDescriptor::decode_descriptor_tree_entry(
                            key, value,
                        ) {
                            Ok(block) => {
                                count = match count.checked_add(u64::from(block.record_count)) {
                                    Some(count) => count,
                                    None => {
                                        decode_error = Some(CanonicalAdjacencyError::Corrupt(
                                            "canonical adjacency estimate overflow".to_string(),
                                        ));
                                        return Ok(GraphDescriptorTreeScanControl::Stop);
                                    }
                                };
                                Ok(GraphDescriptorTreeScanControl::Continue)
                            }
                            Err(error) => {
                                decode_error = Some(error);
                                Ok(GraphDescriptorTreeScanControl::Stop)
                            }
                        }
                    },
                );
                let result = match (result, decode_error) {
                    (_, Some(error)) => Err(error),
                    (Err(error), None) => Err(error.into()),
                    (Ok(_), None) => Ok(count),
                };
                self.poison_on_physical_failure(&result);
                result
            }
        }
    }

    pub fn scan_endpoint_control(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> Result<CanonicalScanControl, CanonicalAdjacencyError>,
    ) -> Result<(CanonicalAdjacencyReadReport, CanonicalScanControl), CanonicalAdjacencyError> {
        self.scan_endpoint_entries_control(endpoint, direction, rel_type, |entry| match entry {
            CanonicalAdjacencyEntry::Inline(relationship) => consumer(relationship),
            CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => {
                Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency relationship {} requires canonical resolution",
                    relationship_id.0
                )))
            }
        })
    }

    pub fn scan_endpoint_entries_control(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(
            CanonicalAdjacencyEntry,
        ) -> Result<CanonicalScanControl, CanonicalAdjacencyError>,
    ) -> Result<(CanonicalAdjacencyReadReport, CanonicalScanControl), CanonicalAdjacencyError> {
        self.ensure_healthy()?;
        let result = match &self.descriptors {
            CanonicalAdjacencyDescriptorBackend::Resident(manifest) => {
                let direction_tagged = direction_tag(direction);
                let start = manifest.blocks.partition_point(|block| {
                    (direction_tag(block.direction), block.endpoint.0)
                        < (direction_tagged, endpoint.0)
                });
                let end = manifest.blocks.partition_point(|block| {
                    (direction_tag(block.direction), block.endpoint.0)
                        <= (direction_tagged, endpoint.0)
                });
                self.scan_blocks(
                    manifest.blocks[start..end]
                        .iter()
                        .filter(|block| rel_type.is_none_or(|expected| block.rel_type == expected)),
                    consumer,
                )
            }
            CanonicalAdjacencyDescriptorBackend::Demand(demand) => {
                let prefix = descriptor_prefix(endpoint, direction, rel_type);
                let mut report = CanonicalAdjacencyReadReport {
                    generation: self.generation.0,
                    ..CanonicalAdjacencyReadReport::default()
                };
                let mut scan_control = CanonicalScanControl::Continue;
                let mut scan_error = None;
                let descriptor_result = demand.scan_prefix(
                    &prefix,
                    GraphDescriptorTreeReadLimits::default(),
                    |key, value| {
                        let step = CanonicalAdjacencyBlockDescriptor::decode_descriptor_tree_entry(
                            key, value,
                        )
                        .and_then(|block| {
                            report.blocks_considered =
                                report.blocks_considered.checked_add(1).ok_or_else(|| {
                                    CanonicalAdjacencyError::Corrupt(
                                        "canonical adjacency block accounting overflow".to_string(),
                                    )
                                })?;
                            self.scan_one_block(&block, &mut report, &mut consumer)
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
                match (descriptor_result, scan_error) {
                    (_, Some(error)) => Err(error),
                    (Err(error), None) => Err(error.into()),
                    (Ok((descriptor_report, _)), None) => {
                        report.record_descriptor_read(descriptor_report);
                        Ok((report, scan_control))
                    }
                }
            }
        };
        self.poison_on_physical_failure(&result);
        result
    }

    fn scan_blocks<'a>(
        &self,
        blocks: impl IntoIterator<Item = &'a CanonicalAdjacencyBlockDescriptor>,
        mut consumer: impl FnMut(
            CanonicalAdjacencyEntry,
        ) -> Result<CanonicalScanControl, CanonicalAdjacencyError>,
    ) -> Result<(CanonicalAdjacencyReadReport, CanonicalScanControl), CanonicalAdjacencyError> {
        let mut report = CanonicalAdjacencyReadReport {
            generation: self.generation.0,
            ..CanonicalAdjacencyReadReport::default()
        };
        for block in blocks {
            report.blocks_considered = report.blocks_considered.saturating_add(1);
            let control = self.scan_one_block(block, &mut report, &mut consumer)?;
            if control == CanonicalScanControl::Stop {
                return Ok((report, control));
            }
        }
        Ok((report, CanonicalScanControl::Continue))
    }

    fn scan_one_block(
        &self,
        block: &CanonicalAdjacencyBlockDescriptor,
        report: &mut CanonicalAdjacencyReadReport,
        consumer: &mut impl FnMut(
            CanonicalAdjacencyEntry,
        ) -> Result<CanonicalScanControl, CanonicalAdjacencyError>,
    ) -> Result<CanonicalScanControl, CanonicalAdjacencyError> {
        let read = self.read_block(block)?;
        report.blocks_read = report.blocks_read.saturating_add(1);
        report.bytes_read = report.bytes_read.saturating_add(read.payload.len() as u64);
        report.cache_hits = report.cache_hits.saturating_add(u64::from(read.cache_hit));
        report.cache_misses = report
            .cache_misses
            .saturating_add(u64::from(read.cache_miss));
        match block.layout {
            AdjacencyLayout::Sparse => {
                report.sparse_blocks_read = report.sparse_blocks_read.saturating_add(1)
            }
            AdjacencyLayout::Dense => {
                report.dense_blocks_read = report.dense_blocks_read.saturating_add(1)
            }
        }
        let mut control = CanonicalScanControl::Continue;
        decode_block(&read.payload, self.generation, block, |relationship| {
            if control == CanonicalScanControl::Stop {
                return Ok(());
            }
            control = consumer(relationship)?;
            report.records_decoded = report.records_decoded.saturating_add(1);
            Ok(())
        })?;
        Ok(control)
    }

    fn read_block(
        &self,
        block: &CanonicalAdjacencyBlockDescriptor,
    ) -> Result<SegmentRangeRead, CanonicalAdjacencyError> {
        if block.length.get() > self.max_block_bytes.get() {
            return Err(CanonicalAdjacencyError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        self.range_reader
            .read_range_with_report(&SegmentReadRange {
                artifact_id: ARTIFACT_ID,
                segment_ids: vec![block.block_id],
                offset: block.offset,
                length: block.length,
                content_digest: Some(block.content_digest),
            })
            .map_err(CanonicalAdjacencyError::from)
    }

    pub fn deep_scrub(&self) -> Result<CanonicalAdjacencyScrubReport, CanonicalAdjacencyError> {
        self.ensure_healthy()?;
        let result = self.deep_scrub_inner();
        self.poison_on_physical_failure(&result);
        result
    }

    fn deep_scrub_inner(&self) -> Result<CanonicalAdjacencyScrubReport, CanonicalAdjacencyError> {
        let adjacency_bytes_hashed = self.verify_whole_artifact()?;
        let mut artifact = File::open(&self.path)?;
        let mut expected_offset = (ARTIFACT_HEADER.len() + 8) as u64;
        let mut expected_block_id = BLOCK_ID_BASE;
        let mut blocks_checked = 0u64;
        let mut records_checked = 0u64;
        let mut visit_block = |block: &CanonicalAdjacencyBlockDescriptor| {
            if block.offset != expected_offset || block.block_id != expected_block_id {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency descriptor closure is not contiguous at block {}",
                    block.block_id
                )));
            }
            let encoded = read_block_uncached(&mut artifact, block, self.max_block_bytes)?;
            let mut decoded = 0u64;
            decode_block(&encoded, self.generation, block, |_| {
                decoded = decoded.checked_add(1).ok_or_else(|| {
                    CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency scrub record count overflow".to_string(),
                    )
                })?;
                Ok(())
            })?;
            records_checked = records_checked.checked_add(decoded).ok_or_else(|| {
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency scrub total record count overflow".to_string(),
                )
            })?;
            blocks_checked = blocks_checked.checked_add(1).ok_or_else(|| {
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency scrub block count overflow".to_string(),
                )
            })?;
            expected_offset = expected_offset
                .checked_add(block.length.get())
                .ok_or_else(|| {
                    CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency scrub artifact offset overflow".to_string(),
                    )
                })?;
            expected_block_id = expected_block_id.checked_add(1).ok_or_else(|| {
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency scrub block id overflow".to_string(),
                )
            })?;
            Ok(())
        };

        let (descriptor_pages_checked, descriptors_checked, descriptor_bytes_checked) = match &self
            .descriptors
        {
            CanonicalAdjacencyDescriptorBackend::Resident(manifest) => {
                for block in &manifest.blocks {
                    visit_block(block)?;
                }
                (0, manifest.blocks.len() as u64, 0)
            }
            CanonicalAdjacencyDescriptorBackend::Demand(demand) => {
                let mut visit_error = None;
                let scrub = demand.deep_visit(|key, value| {
                    let result =
                        CanonicalAdjacencyBlockDescriptor::decode_descriptor_tree_entry(key, value)
                            .and_then(|block| visit_block(&block));
                    match result {
                        Ok(()) => Ok(GraphDescriptorTreeScanControl::Continue),
                        Err(error) => {
                            visit_error = Some(error);
                            Ok(GraphDescriptorTreeScanControl::Stop)
                        }
                    }
                });
                match (scrub, visit_error) {
                    (_, Some(error)) => return Err(error),
                    (Err(error), None) => return Err(error.into()),
                    (Ok(report), None) => (
                        report.checked_pages,
                        report.checked_descriptors,
                        report.page_bytes_decoded,
                    ),
                }
            }
        };
        if expected_offset != self.artifact_len
            || records_checked != self.entry_count
            || blocks_checked != descriptors_checked
        {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency scrub closure bytes/records/blocks {expected_offset}/{records_checked}/{blocks_checked} do not match {}/{}/{}",
                self.artifact_len, self.entry_count, descriptors_checked
            )));
        }
        Ok(CanonicalAdjacencyScrubReport {
            descriptor_pages_checked,
            descriptors_checked,
            descriptor_bytes_checked,
            adjacency_blocks_checked: blocks_checked,
            adjacency_records_checked: records_checked,
            adjacency_bytes_hashed,
        })
    }

    fn verify_whole_artifact(&self) -> Result<u64, CanonicalAdjacencyError> {
        let mut file = File::open(&self.path)?;
        if file.metadata()?.len() != self.artifact_len {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact length changed after open".to_string(),
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
                CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency scrub byte count overflow".to_string(),
                )
            })?;
        }
        let digest = hasher.finish();
        if digest.crc32c.as_u64() != self.artifact_digest.0 || digest.sha256 != self.artifact_sha256
        {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact checksum mismatch during scrub".to_string(),
            ));
        }
        Ok(total)
    }

    fn ensure_healthy(&self) -> Result<(), CanonicalAdjacencyError> {
        if self.is_poisoned() {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency reader is poisoned by an earlier physical failure".to_string(),
            ));
        }
        Ok(())
    }

    fn poison_on_physical_failure<T>(&self, result: &Result<T, CanonicalAdjacencyError>) {
        if result.as_ref().is_err_and(error_requires_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

impl CanonicalAdjacencyReadReport {
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

fn descriptor_prefix(
    endpoint: NodeId,
    direction: AdjacencyDirection,
    rel_type: Option<RelTypeId>,
) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(if rel_type.is_some() { 13 } else { 9 });
    prefix.push(direction_tag(direction));
    prefix.extend_from_slice(&endpoint.0.to_be_bytes());
    if let Some(rel_type) = rel_type {
        prefix.extend_from_slice(&rel_type.0.to_be_bytes());
    }
    prefix
}

fn validate_artifact_header(
    path: &Path,
    generation: u64,
    expected_len: u64,
) -> Result<(), CanonicalAdjacencyError> {
    let metadata = fs::metadata(path)?;
    if metadata.len() != expected_len {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency artifact length mismatch: expected {expected_len}, got {}",
            metadata.len()
        )));
    }
    let mut header = [0u8; 24];
    File::open(path)?.read_exact(&mut header)?;
    if &header[..16] != ARTIFACT_HEADER {
        return Err(CanonicalAdjacencyError::Corrupt(
            "canonical adjacency artifact has an invalid header".to_string(),
        ));
    }
    let stored_generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
    if stored_generation != generation {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency artifact generation {stored_generation} does not match binding {generation}"
        )));
    }
    Ok(())
}

fn read_block_uncached(
    file: &mut File,
    block: &CanonicalAdjacencyBlockDescriptor,
    max_block_bytes: NonZeroU64,
) -> Result<Vec<u8>, CanonicalAdjacencyError> {
    if block.length.get() > max_block_bytes.get() {
        return Err(CanonicalAdjacencyError::BlockTooLarge {
            block_bytes: block.length.get(),
            max_bytes: max_block_bytes.get(),
        });
    }
    let length = usize::try_from(block.length.get()).map_err(|_| {
        CanonicalAdjacencyError::BlockTooLarge {
            block_bytes: block.length.get(),
            max_bytes: usize::MAX as u64,
        }
    })?;
    file.seek(SeekFrom::Start(block.offset))?;
    let mut encoded = vec![0u8; length];
    file.read_exact(&mut encoded)?;
    if content_digest(&encoded) != block.content_digest {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} failed content digest verification",
            block.block_id
        )));
    }
    Ok(encoded)
}

fn error_requires_poison(error: &CanonicalAdjacencyError) -> bool {
    match error {
        CanonicalAdjacencyError::Io(_)
        | CanonicalAdjacencyError::Read(_)
        | CanonicalAdjacencyError::Corrupt(_) => true,
        CanonicalAdjacencyError::DescriptorTree(error) => matches!(
            error,
            GraphDescriptorTreeError::Io(_)
                | GraphDescriptorTreeError::Page(GraphDescriptorPageError::Corrupt(_))
                | GraphDescriptorTreeError::Corrupt(_)
        ),
        CanonicalAdjacencyError::Source(_)
        | CanonicalAdjacencyError::RecordTooLarge { .. }
        | CanonicalAdjacencyError::MemoryBudgetExceeded { .. }
        | CanonicalAdjacencyError::SpillBudgetExceeded { .. }
        | CanonicalAdjacencyError::SpillRunBudgetExceeded { .. }
        | CanonicalAdjacencyError::BlockTooLarge { .. } => false,
    }
}

fn reject_existing_immutable_artifact(
    path: &Path,
    artifact: &str,
) -> Result<(), CanonicalAdjacencyError> {
    if path.exists() {
        return Err(CanonicalAdjacencyError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "immutable {artifact} artifact {} already exists",
                path.display()
            ),
        )));
    }
    Ok(())
}

fn decode_block(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalAdjacencyBlockDescriptor,
    mut consumer: impl FnMut(CanonicalAdjacencyEntry) -> Result<(), CanonicalAdjacencyError>,
) -> Result<(), CanonicalAdjacencyError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} has an invalid header",
            descriptor.block_id
        )));
    }
    let stored_generation = cursor.read_u64()?;
    let block_id = cursor.read_u64()?;
    let direction = direction_from_tag(cursor.read_u8()?)?;
    let layout = layout_from_tag(cursor.read_u8()?)?;
    let endpoint = NodeId(cursor.read_u64()?);
    let rel_type = RelTypeId(cursor.read_u32()?);
    let record_count = cursor.read_u32()?;
    if stored_generation != generation.0
        || block_id != descriptor.block_id
        || direction != descriptor.direction
        || layout != descriptor.layout
        || endpoint != descriptor.endpoint
        || rel_type != descriptor.rel_type
        || record_count != descriptor.record_count
    {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} metadata does not match its manifest",
            descriptor.block_id
        )));
    }
    let mut previous = None;
    for _ in 0..record_count {
        let neighbor = NodeId(cursor.read_u64()?);
        let rel_id = cursor.read_u64()?;
        let payload_len = cursor.read_u32()? as usize;
        let payload = cursor.read_exact(payload_len)?;
        let key = (neighbor.0, rel_id);
        if previous.is_some_and(|previous| previous >= key) {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency block {} records are not strictly ordered",
                descriptor.block_id
            )));
        }
        if payload.is_empty() {
            consumer(CanonicalAdjacencyEntry::CanonicalReference {
                relationship_id: crate::RelId(rel_id),
            })?;
        } else {
            let relationship = decode_relationship(rel_id, payload)
                .map_err(|error| CanonicalAdjacencyError::Corrupt(error.to_string()))?;
            let (actual_endpoint, actual_neighbor) = match direction {
                AdjacencyDirection::Outgoing => (relationship.source, relationship.target),
                AdjacencyDirection::Incoming => (relationship.target, relationship.source),
            };
            if actual_endpoint != endpoint
                || actual_neighbor != neighbor
                || relationship.rel_type != rel_type
            {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency block {} relationship {} does not match its key",
                    descriptor.block_id, rel_id
                )));
            }
            consumer(CanonicalAdjacencyEntry::Inline(relationship))?;
        }
        previous = Some(key);
    }
    if !cursor.is_empty()
        || previous.is_none()
        || previous.map(|value| NodeId(value.0)) != Some(descriptor.max_neighbor)
    {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} payload bounds are inconsistent",
            descriptor.block_id
        )));
    }
    Ok(())
}

fn descriptor_key(block: &CanonicalAdjacencyBlockDescriptor) -> (u8, u64, u32, u64, u64) {
    (
        direction_tag(block.direction),
        block.endpoint.0,
        block.rel_type.0,
        block.min_neighbor.0,
        block.block_id,
    )
}

fn write_entry(
    writer: &mut impl Write,
    entry: &EncodedEntry,
) -> Result<(), CanonicalAdjacencyError> {
    writer.write_all(&[entry.key.direction])?;
    writer.write_all(&entry.key.endpoint.to_le_bytes())?;
    writer.write_all(&entry.key.rel_type.to_le_bytes())?;
    writer.write_all(&entry.key.neighbor.to_le_bytes())?;
    writer.write_all(&entry.key.rel_id.to_le_bytes())?;
    writer.write_all(
        &u32::try_from(entry.payload.len())
            .map_err(|_| CanonicalAdjacencyError::RecordTooLarge {
                record_bytes: entry.payload.len() as u64,
                max_bytes: u64::from(u32::MAX),
            })?
            .to_le_bytes(),
    )?;
    writer.write_all(&entry.payload)?;
    Ok(())
}

fn estimated_relationship_payload_bytes(relationship: &RelRecord) -> u64 {
    8u64.saturating_add(8)
        .saturating_add(4)
        .saturating_add(4)
        .saturating_add(
            relationship
                .properties
                .iter()
                .map(|(key, value)| {
                    4u64.saturating_add(key.len() as u64)
                        .saturating_add(estimated_value_bytes(value))
                })
                .fold(0u64, u64::saturating_add),
        )
}

fn estimated_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Int(_) | Value::Float(_) => 9,
        Value::String(value) => 5u64.saturating_add(value.len() as u64),
        Value::Binary(value) => 5u64.saturating_add(value.len() as u64),
        Value::Uuid(_) => 17,
        Value::List(values) => 5u64.saturating_add(
            values
                .iter()
                .map(estimated_value_bytes)
                .fold(0u64, u64::saturating_add),
        ),
        Value::Map(values) => 5u64.saturating_add(
            values
                .iter()
                .map(|(key, value)| {
                    4u64.saturating_add(key.len() as u64)
                        .saturating_add(estimated_value_bytes(value))
                })
                .fold(0u64, u64::saturating_add),
        ),
    }
}

fn direction_tag(direction: AdjacencyDirection) -> u8 {
    match direction {
        AdjacencyDirection::Outgoing => 1,
        AdjacencyDirection::Incoming => 2,
    }
}

fn direction_from_tag(tag: u8) -> Result<AdjacencyDirection, CanonicalAdjacencyError> {
    match tag {
        1 => Ok(AdjacencyDirection::Outgoing),
        2 => Ok(AdjacencyDirection::Incoming),
        _ => Err(CanonicalAdjacencyError::Corrupt(format!(
            "invalid canonical adjacency direction {tag}"
        ))),
    }
}

fn layout_tag(layout: AdjacencyLayout) -> u8 {
    match layout {
        AdjacencyLayout::Sparse => 1,
        AdjacencyLayout::Dense => 2,
    }
}

fn layout_from_tag(tag: u8) -> Result<AdjacencyLayout, CanonicalAdjacencyError> {
    match tag {
        1 => Ok(AdjacencyLayout::Sparse),
        2 => Ok(AdjacencyLayout::Dense),
        _ => Err(CanonicalAdjacencyError::Corrupt(format!(
            "invalid canonical adjacency layout {tag}"
        ))),
    }
}

fn parse_u64(value: &str, name: &str) -> Result<u64, CanonicalAdjacencyError> {
    value.parse().map_err(|_| {
        CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency manifest has an invalid {name}: {value}"
        ))
    })
}

fn parse_u32(value: &str, name: &str) -> Result<u32, CanonicalAdjacencyError> {
    value.parse().map_err(|_| {
        CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency manifest has an invalid {name}: {value}"
        ))
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, CanonicalAdjacencyError> {
    value.ok_or_else(|| {
        CanonicalAdjacencyError::Corrupt(format!("canonical adjacency manifest is missing {name}"))
    })
}

fn read_u32(reader: &mut impl Read) -> Result<u32, CanonicalAdjacencyError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, CanonicalAdjacencyError> {
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

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalAdjacencyError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency cursor offset overflow".to_string(),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency block ended before its declared length".to_string(),
            )
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, CanonicalAdjacencyError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CanonicalAdjacencyError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, CanonicalAdjacencyError> {
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
) -> Result<(), CanonicalAdjacencyError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut IntegrityHasher,
    block_digest: &mut Crc32cHasher,
    bytes: &[u8],
) -> Result<(), CanonicalAdjacencyError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelId;
    use std::collections::BTreeMap;

    fn test_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein-canonical-adjacency-{name}-{}-{nonce}",
            std::process::id(),
        ))
    }

    fn relationship(id: u64, source: u64, target: u64, rel_type: u32) -> RelRecord {
        RelRecord {
            id: RelId(id),
            source: NodeId(source),
            target: NodeId(target),
            rel_type: RelTypeId(rel_type),
            properties: BTreeMap::new(),
        }
    }

    #[test]
    fn external_sort_builds_sparse_and_dense_endpoint_blocks() {
        let root = test_path("sparse-dense");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("adjacency.skein");
        let config = CanonicalAdjacencyConfig {
            memory_budget_bytes: NonZeroU64::new(128).unwrap(),
            target_block_bytes: NonZeroU64::new(256).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            dense_degree_threshold: NonZeroUsize::new(4).unwrap(),
            ..CanonicalAdjacencyConfig::default()
        };
        let relationships = vec![
            relationship(7, 1, 17, 3),
            relationship(2, 1, 12, 3),
            relationship(9, 2, 19, 3),
            relationship(1, 1, 11, 3),
            relationship(5, 1, 15, 3),
        ];
        let output = CanonicalAdjacencyWriter::new(config)
            .write_fallible(
                &path,
                ManifestGeneration(4),
                relationships.into_iter().map(Ok),
            )
            .unwrap();
        assert!(output.descriptor_tree.is_none());
        assert!(output.report.spill_run_count > 1);
        assert!(output.report.sparse_block_count > 0);
        assert!(output.report.dense_block_count > 1);
        let resident_manifest = output
            .resident_manifest
            .as_ref()
            .expect("resident writer emits a manifest for codec tests");
        let manifest =
            CanonicalAdjacencyManifest::decode(&resident_manifest.encode().unwrap()).unwrap();
        let cache = Arc::new(SegmentCache::new(1024 * 1024));
        let reader = CanonicalAdjacencyReader::open(
            &path,
            manifest,
            cache,
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let mut ids = Vec::new();
        let (report, control) = reader
            .scan_endpoint_control(
                NodeId(1),
                AdjacencyDirection::Outgoing,
                Some(RelTypeId(3)),
                |relationship| {
                    ids.push(relationship.id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(ids, vec![1, 2, 5, 7]);
        assert_eq!(report.records_decoded, 4);
        assert!(report.dense_blocks_read > 1);
        assert!(fs::read_dir(&root).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".run.")));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn descriptor_tree_entry_codec_is_order_preserving_and_symmetric() {
        let first = CanonicalAdjacencyBlockDescriptor {
            block_id: BLOCK_ID_BASE,
            direction: AdjacencyDirection::Outgoing,
            layout: AdjacencyLayout::Sparse,
            endpoint: NodeId(9),
            rel_type: RelTypeId(3),
            min_neighbor: NodeId(10),
            max_neighbor: NodeId(20),
            offset: 48,
            length: NonZeroU64::new(512).unwrap(),
            content_digest: ContentDigest(17),
            record_count: 4,
        };
        let mut second = first.clone();
        second.block_id += 1;
        second.min_neighbor = NodeId(21);
        second.max_neighbor = NodeId(30);
        assert!(first.descriptor_tree_key() < second.descriptor_tree_key());
        assert_eq!(
            CanonicalAdjacencyBlockDescriptor::decode_descriptor_tree_entry(
                &first.descriptor_tree_key(),
                &first.encode_descriptor_tree_value(),
            )
            .unwrap(),
            first
        );

        let mut drifted_key = first.descriptor_tree_key();
        drifted_key[20] ^= 1;
        assert!(
            CanonicalAdjacencyBlockDescriptor::decode_descriptor_tree_entry(
                &drifted_key,
                &first.encode_descriptor_tree_value(),
            )
            .is_err()
        );
    }

    #[test]
    fn writer_publishes_descriptor_root_after_adjacency_artifact() {
        let root = test_path("descriptor-root");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let adjacency_path = root.join("adjacency.7.skein");
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(canonical_adjacency_descriptor_page_file(7)),
            root.join(canonical_adjacency_descriptor_root_file(7)),
        );
        let descriptor_config = GraphDescriptorTreeBuildConfig {
            page_limits: crate::GraphDescriptorPageLimits {
                max_page_bytes: NonZeroUsize::new(512).unwrap(),
                max_entries: NonZeroUsize::new(4).unwrap(),
                max_key_bytes: NonZeroUsize::new(64).unwrap(),
                max_value_bytes: NonZeroUsize::new(128).unwrap(),
            },
            max_root_bytes: NonZeroUsize::new(4096).unwrap(),
            max_page_count: NonZeroU64::new(4096).unwrap(),
            max_page_artifact_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            max_intermediate_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        };
        let relationships = (0..128).map(|index| {
            Ok(relationship(
                index + 1,
                index % 11 + 1,
                index + 100,
                (index % 3 + 1) as u32,
            ))
        });
        let output = CanonicalAdjacencyWriter::new(CanonicalAdjacencyConfig::default())
            .write_fallible_with_descriptor_tree(
                &adjacency_path,
                descriptor_paths.clone(),
                ManifestGeneration(7),
                29,
                descriptor_config,
                relationships,
            )
            .unwrap();
        let descriptor_tree = output
            .descriptor_tree
            .expect("descriptor tree is published");
        assert_eq!(
            descriptor_tree.root.descriptor_count,
            output
                .report
                .sparse_block_count
                .checked_add(output.report.dense_block_count)
                .unwrap()
        );
        assert!(adjacency_path.exists());
        let reader =
            crate::GraphDescriptorTreeRootReader::open(descriptor_paths, descriptor_config)
                .unwrap();
        assert_eq!(reader.root(), &descriptor_tree.root);
        assert_eq!(reader.report().page_payload_bytes_read, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writer_refuses_to_replace_an_existing_adjacency_generation() {
        let root = test_path("immutable-generation");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let adjacency_path = root.join("adjacency.7.skein");
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(canonical_adjacency_descriptor_page_file(7)),
            root.join(canonical_adjacency_descriptor_root_file(7)),
        );
        let writer = CanonicalAdjacencyWriter::new(CanonicalAdjacencyConfig::default());
        writer
            .write_fallible_with_descriptor_tree(
                &adjacency_path,
                descriptor_paths.clone(),
                ManifestGeneration(7),
                29,
                GraphDescriptorTreeBuildConfig::default(),
                [Ok(relationship(1, 1, 2, 3))],
            )
            .unwrap();
        let adjacency_before = fs::read(&adjacency_path).unwrap();
        let pages_before = fs::read(&descriptor_paths.page_artifact).unwrap();
        let root_before = fs::read(&descriptor_paths.root_manifest).unwrap();

        let error = writer
            .write_fallible_with_descriptor_tree(
                &adjacency_path,
                descriptor_paths.clone(),
                ManifestGeneration(7),
                30,
                GraphDescriptorTreeBuildConfig::default(),
                [Ok(relationship(2, 3, 4, 3))],
            )
            .expect_err("an existing adjacency generation must be immutable");
        assert_eq!(
            error.to_string(),
            format!(
                "immutable canonical adjacency data artifact {} already exists",
                adjacency_path.display()
            )
        );
        assert_eq!(fs::read(&adjacency_path).unwrap(), adjacency_before);
        assert_eq!(
            fs::read(&descriptor_paths.page_artifact).unwrap(),
            pages_before
        );
        assert_eq!(
            fs::read(&descriptor_paths.root_manifest).unwrap(),
            root_before
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn demand_reader_matches_resident_manifest_and_scrubs_without_cache_warming() {
        let root = test_path("demand-reader");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let adjacency_path = root.join("adjacency.7.skein");
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(canonical_adjacency_descriptor_page_file(7)),
            root.join(canonical_adjacency_descriptor_root_file(7)),
        );
        let descriptor_config = GraphDescriptorTreeBuildConfig {
            page_limits: crate::GraphDescriptorPageLimits {
                max_page_bytes: NonZeroUsize::new(512).unwrap(),
                max_entries: NonZeroUsize::new(4).unwrap(),
                max_key_bytes: NonZeroUsize::new(64).unwrap(),
                max_value_bytes: NonZeroUsize::new(128).unwrap(),
            },
            max_root_bytes: NonZeroUsize::new(4096).unwrap(),
            max_page_count: NonZeroU64::new(4096).unwrap(),
            max_page_artifact_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            max_intermediate_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        };
        let relationships = (0..256)
            .map(|index| {
                relationship(
                    index + 1,
                    index % 7 + 1,
                    index + 100,
                    (index % 3 + 1) as u32,
                )
            })
            .collect::<Vec<_>>();
        let expected_ids = relationships
            .iter()
            .filter(|relationship| {
                relationship.source == NodeId(1) && relationship.rel_type == RelTypeId(1)
            })
            .map(|relationship| relationship.id)
            .collect::<Vec<_>>();
        let output = CanonicalAdjacencyWriter::new(CanonicalAdjacencyConfig {
            target_block_bytes: NonZeroU64::new(256).unwrap(),
            dense_degree_threshold: NonZeroUsize::new(4).unwrap(),
            ..CanonicalAdjacencyConfig::default()
        })
        .write_fallible_with_descriptor_tree(
            &adjacency_path,
            descriptor_paths.clone(),
            ManifestGeneration(7),
            29,
            descriptor_config,
            relationships.clone().into_iter().map(Ok),
        )
        .unwrap();
        let binding = output.generation_artifacts().unwrap();
        let max_block_bytes = NonZeroU64::new(16 * 1024 * 1024).unwrap();
        assert!(output.resident_manifest.is_none());
        let cache = Arc::new(SegmentCache::new(128 * 1024));
        let root_reader = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths.clone(),
            output
                .descriptor_tree
                .as_ref()
                .unwrap()
                .generation_artifacts(),
            descriptor_config,
        )
        .unwrap();
        let demand = CanonicalAdjacencyReader::open_demand_paged(
            &adjacency_path,
            binding,
            root_reader,
            descriptor_config,
            Arc::clone(&cache),
            StoreId(41),
            max_block_bytes,
        )
        .unwrap();

        let collect = |reader: &CanonicalAdjacencyReader| {
            let mut ids = Vec::new();
            let (report, control) = reader
                .scan_endpoint_control(
                    NodeId(1),
                    AdjacencyDirection::Outgoing,
                    Some(RelTypeId(1)),
                    |relationship| {
                        ids.push(relationship.id);
                        Ok(CanonicalScanControl::Continue)
                    },
                )
                .unwrap();
            assert_eq!(control, CanonicalScanControl::Continue);
            (ids, report)
        };
        let (cold_ids, cold) = collect(&demand);
        assert_eq!(cold_ids, expected_ids);
        assert_eq!(cold.generation, 7);
        assert!(cold.descriptor_pages_visited > 0);
        assert!(cold.descriptor_page_bytes_decoded > 0);
        assert!(cold.descriptor_storage_bytes_read > 0);
        assert!(cold.descriptor_cache_misses > 0);
        assert_eq!(
            demand
                .estimate_endpoint_entries(
                    NodeId(1),
                    AdjacencyDirection::Outgoing,
                    Some(RelTypeId(1)),
                )
                .unwrap(),
            expected_ids.len() as u64
        );

        let (_, warm) = collect(&demand);
        assert!(warm.descriptor_cache_hits > 0);
        assert_eq!(
            warm.descriptor_page_bytes_decoded,
            cold.descriptor_page_bytes_decoded
        );
        assert_eq!(warm.descriptor_storage_bytes_read, 0);
        let mut stopped_ids = Vec::new();
        let (stopped, control) = demand
            .scan_endpoint_control(
                NodeId(1),
                AdjacencyDirection::Outgoing,
                Some(RelTypeId(1)),
                |relationship| {
                    stopped_ids.push(relationship.id);
                    Ok(CanonicalScanControl::Stop)
                },
            )
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Stop);
        assert_eq!(stopped_ids.len(), 1);
        assert_eq!(stopped.records_decoded, 1);

        let cache_before_scrub = cache.snapshot();
        let scrub = demand.deep_scrub().unwrap();
        assert_eq!(
            scrub.descriptors_checked,
            output
                .report
                .sparse_block_count
                .checked_add(output.report.dense_block_count)
                .unwrap()
        );
        assert_eq!(scrub.adjacency_records_checked, binding.entry_count);
        assert_eq!(
            scrub.adjacency_bytes_hashed,
            binding.adjacency_artifact.encoded_len
        );
        assert_eq!(cache.snapshot(), cache_before_scrub);

        let admission_root = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            output
                .descriptor_tree
                .as_ref()
                .unwrap()
                .generation_artifacts(),
            descriptor_config,
        )
        .unwrap();
        let admission_reader = CanonicalAdjacencyReader::open_demand_paged(
            &adjacency_path,
            binding,
            admission_root,
            descriptor_config,
            Arc::new(SegmentCache::new(128 * 1024)),
            StoreId(42),
            NonZeroU64::new(1).unwrap(),
        )
        .unwrap();
        let error = admission_reader
            .scan_endpoint_control(
                NodeId(1),
                AdjacencyDirection::Outgoing,
                Some(RelTypeId(1)),
                |_| Ok(CanonicalScanControl::Continue),
            )
            .expect_err("block byte admission must reject before allocation");
        assert!(matches!(
            error,
            CanonicalAdjacencyError::BlockTooLarge { .. }
        ));
        assert!(!admission_reader.is_poisoned());
        fs::remove_dir_all(root).unwrap();
    }
}
