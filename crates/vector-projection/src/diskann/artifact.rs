// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Fixed-width, immutable vector/link records. The enclosing Consumer owns
//! file custody, durability, publication and the authoritative source binding.

use super::*;
use crate::ProjectionReadAdmission;
use crc32fast::Hasher;
#[cfg(feature = "storage-io")]
use hawdb_storage::file_io::File;
use memmap2::Mmap;
#[cfg(not(feature = "storage-io"))]
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

const MAGIC: &[u8; 8] = b"HDANN002";
const HEADER_BYTES: usize = 144;
const FOOTER_BYTES: usize = 12;
const VERIFY_BYTES: usize = 16 << 10;
const RECORD_PREFIX_BYTES: usize = 24;

#[cfg(test)]
mod tests;

/// Artifact-local identity; matching it is necessary, never sufficient serving
/// authority. The Consumer must independently validate canonical source custody.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskAnnArtifactIdentity {
    pub source_digest: [u8; 32],
    /// Caller-defined immutable namespace, such as an embedding model/version.
    pub binding_digest: [u8; 32],
    pub source_epoch: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskAnnArtifactMetadata {
    pub identity: DiskAnnArtifactIdentity,
    pub dimension: usize,
    pub metric: VectorDistance,
    pub node_count: usize,
    pub max_degree: usize,
    pub build_beam: usize,
    pub alpha: f64,
    pub lsg: Option<LsgBuildConfig>,
    pub entry_point: Option<u32>,
}

#[derive(Debug)]
pub struct FileDiskAnn<R> {
    contents: Mmap,
    metadata: DiskAnnArtifactMetadata,
    #[cfg(feature = "storage-io")]
    artifact_checksum: u32,
    _memory: R,
}

impl DiskAnnIndex {
    /// Write into an already admitted private artifact. The caller must sync
    /// this complete file before publishing the enclosing generation selector.
    pub fn write_artifact(
        &self,
        output: &mut impl Write,
        identity: DiskAnnArtifactIdentity,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<DiskAnnArtifactMetadata> {
        checkpoint(task)?;
        let config = self.config();
        let metadata = DiskAnnArtifactMetadata {
            identity,
            dimension: self.dimension(),
            metric: self.metric(),
            node_count: self.len(),
            max_degree: self.max_degree(),
            build_beam: config.build_beam,
            alpha: config.alpha,
            lsg: config.lsg,
            entry_point: self.entry_point(),
        };
        let header = metadata.encode()?;
        let mut checksum = Hasher::new();
        let mut write = |bytes: &[u8]| -> Result<()> {
            output.write_all(bytes)?;
            checksum.update(bytes);
            Ok(())
        };
        write(&header)?;
        for ordinal in 0..self.len() as u32 {
            checkpoint(task)?;
            let neighbors = self.neighbors(ordinal)?;
            let degree = neighbors.count();
            write(&self.id(ordinal)?.to_le_bytes())?;
            write(&self.local_radius(ordinal).unwrap_or(1.0).to_le_bytes())?;
            write(&u32_value(degree)?.to_le_bytes())?;
            // This prefix makes the coordinates a canonical zero-copy VectorRef.
            write(&u32_value(self.dimension())?.to_le_bytes())?;
            for (position, coordinate) in self.vector(ordinal)?.iter().enumerate() {
                if position.is_multiple_of(4096) {
                    checkpoint(task)?;
                }
                write(&coordinate.to_le_bytes())?;
            }
            for neighbor in self.neighbors(ordinal)? {
                write(&neighbor.to_le_bytes())?;
            }
            for position in degree..self.max_degree() {
                if position.is_multiple_of(4096) {
                    checkpoint(task)?;
                }
                write(&u32::MAX.to_le_bytes())?;
            }
        }
        checkpoint(task)?;
        output.write_all(&checksum.finalize().to_le_bytes())?;
        output.write_all(MAGIC)?;
        Ok(metadata)
    }
}

impl FileDiskAnn<()> {
    /// Inspect bounded header metadata without mapping or granting serving authority.
    pub fn inspect_file(input: &mut File) -> Result<DiskAnnArtifactMetadata> {
        input.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; HEADER_BYTES];
        input.read_exact(&mut header)?;
        DiskAnnArtifactMetadata::decode(&header)
    }

    pub fn open_file(input: File, expected: DiskAnnArtifactIdentity) -> Result<Self> {
        Self::open_admitted_file(input, expected, &crate::artifact::Unaccounted)
    }
    /// The supplied file must have an admitted native handle and immutable
    /// lifetime. Metadata/verification work is charged before allocation/map.
    pub fn open_admitted_file<A: ProjectionReadAdmission>(
        mut input: File,
        expected: DiskAnnArtifactIdentity,
        admission: &A,
    ) -> Result<FileDiskAnn<A::Retained>> {
        admission.checkpoint()?;
        let _io = admission.acquire_io()?;
        let _scratch = admission.reserve_scratch(VERIFY_BYTES)?;
        let length = usize::try_from(input.metadata()?.len())
            .map_err(|_| corrupt("DiskANN file exceeds the address space"))?;
        if length < HEADER_BYTES + FOOTER_BYTES {
            return Err(corrupt("truncated DiskANN artifact"));
        }
        input.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; HEADER_BYTES];
        input.read_exact(&mut header)?;
        let metadata = DiskAnnArtifactMetadata::decode(&header)?;
        if metadata.identity != expected {
            return Err(corrupt(
                "DiskANN artifact identity differs from its pinned source",
            ));
        }
        if metadata.file_bytes()? != length {
            return Err(corrupt("DiskANN artifact length differs from its layout"));
        }
        let memory = admission.reserve_retained(size_of::<FileDiskAnn<A::Retained>>())?;
        let mut checksum = Hasher::new();
        checksum.update(&header);
        #[cfg(feature = "storage-io")]
        let mut artifact_checksum = hawdb_integrity::Crc32cHasher::new();
        #[cfg(feature = "storage-io")]
        artifact_checksum.update(&header);
        let mut remaining = length - HEADER_BYTES - FOOTER_BYTES;
        let mut buffer = [0u8; VERIFY_BYTES];
        while remaining != 0 {
            admission.checkpoint()?;
            let count = remaining.min(buffer.len());
            input.read_exact(&mut buffer[..count])?;
            checksum.update(&buffer[..count]);
            #[cfg(feature = "storage-io")]
            artifact_checksum.update(&buffer[..count]);
            remaining -= count;
        }
        let mut footer = [0u8; FOOTER_BYTES];
        input.read_exact(&mut footer)?;
        if &footer[4..] != MAGIC || read_u32(&footer, 0)? != checksum.finalize() {
            return Err(corrupt("DiskANN artifact checksum or footer mismatch"));
        }
        #[cfg(feature = "storage-io")]
        artifact_checksum.update(&footer);
        admission.checkpoint()?;
        // SAFETY: the caller supplies an immutable source-held handle. Its
        // complete length/layout and checksum were validated before mapping.
        #[cfg(feature = "storage-io")]
        let contents = unsafe { input.map_read_only()? };
        // SAFETY: same immutable-handle contract as the storage-backed mapping.
        #[cfg(not(feature = "storage-io"))]
        let contents = unsafe { Mmap::map(&input)? };
        let graph = FileDiskAnn {
            contents,
            metadata,
            #[cfg(feature = "storage-io")]
            artifact_checksum: artifact_checksum.finish() as u32,
            _memory: memory,
        };
        graph.validate(admission)?;
        Ok(graph)
    }
}

impl<R> FileDiskAnn<R> {
    pub fn metadata(&self) -> &DiskAnnArtifactMetadata {
        &self.metadata
    }

    /// Diagnostic length and CRC32C of the complete artifact, captured during
    /// admitted verification. This grants no independent source authority.
    #[cfg(feature = "storage-io")]
    pub fn artifact_fingerprint(&self) -> (u64, u32) {
        (self.contents.len() as u64, self.artifact_checksum)
    }

    /// Copy a verified immutable graph into a private successor artifact.
    /// The caller owns source authority, publication and the new identity.
    pub fn write_artifact(
        &self,
        output: &mut impl Write,
        identity: DiskAnnArtifactIdentity,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<DiskAnnArtifactMetadata> {
        checkpoint(task)?;
        let metadata = DiskAnnArtifactMetadata {
            identity,
            ..self.metadata
        };
        let header = metadata.encode()?;
        let mut checksum = Hasher::new();
        output.write_all(&header)?;
        checksum.update(&header);
        for chunk in
            self.contents[HEADER_BYTES..self.contents.len() - FOOTER_BYTES].chunks(VERIFY_BYTES)
        {
            checkpoint(task)?;
            output.write_all(chunk)?;
            checksum.update(chunk);
        }
        checkpoint(task)?;
        output.write_all(&checksum.finalize().to_le_bytes())?;
        output.write_all(MAGIC)?;
        Ok(metadata)
    }

    fn record(&self, ordinal: u32) -> Result<&[u8]> {
        if ordinal as usize >= self.metadata.node_count {
            return Err(corrupt("DiskANN ordinal is outside its pinned graph"));
        }
        let stride = self.metadata.record_bytes()?;
        let start = HEADER_BYTES + ordinal as usize * stride;
        self.contents
            .get(start..start + stride)
            .ok_or_else(|| corrupt("DiskANN record exceeds its artifact"))
    }

    fn vector(
        &self,
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<hawdb_core::VectorRef<'_>> {
        let record = self.record(ordinal)?;
        hawdb_core::VectorRef::from_encoded_with_work(
            &record[20..RECORD_PREFIX_BYTES + self.dimension() * 4],
            || checkpoint(task),
            |_| Ok(()),
            |_| corrupt("DiskANN record contains an invalid vector"),
        )
    }

    fn validate(&self, admission: &impl ProjectionReadAdmission) -> Result<()> {
        let mut previous = None;
        for ordinal in 0..self.len() as u32 {
            admission.checkpoint()?;
            let id = self.id(ordinal)?;
            if previous.is_some_and(|previous| previous >= id) {
                return Err(corrupt("DiskANN IDs must be sorted and unique"));
            }
            previous = Some(id);
            let record = self.record(ordinal)?;
            let radius = read_f64(record, 8)?;
            if !radius.is_finite() || radius <= 0.0 {
                return Err(corrupt("DiskANN local radius must be positive and finite"));
            }
            if read_u32(record, 20)? as usize != self.dimension() {
                return Err(corrupt("DiskANN record dimension mismatch"));
            }
            let encoded = &record[20..RECORD_PREFIX_BYTES + self.dimension() * 4];
            let vector = hawdb_core::VectorRef::from_encoded_with_work(
                encoded,
                || admission.checkpoint(),
                |_| Ok(()),
                |_| corrupt("DiskANN record contains an invalid vector"),
            )?;
            if self.metric() == VectorDistance::Cosine {
                let mut nonzero = false;
                for (position, coordinate) in vector.coordinates().enumerate() {
                    if position.is_multiple_of(4096) {
                        admission.checkpoint()?;
                    }
                    nonzero |= coordinate != 0.0;
                }
                if !nonzero {
                    return Err(corrupt("DiskANN cosine vector has zero norm"));
                }
            }
            let degree = read_u32(record, 16)? as usize;
            if degree > self.max_degree() || (self.len() > 1 && degree == 0) {
                return Err(corrupt("DiskANN record degree exceeds its graph bound"));
            }
            let mut previous_neighbor = None;
            for (position, chunk) in record[RECORD_PREFIX_BYTES + self.dimension() * 4..]
                .chunks_exact(4)
                .enumerate()
            {
                if position.is_multiple_of(4096) {
                    admission.checkpoint()?;
                }
                let neighbor = read_u32(chunk, 0)?;
                if position < degree {
                    if neighbor as usize >= self.len()
                        || neighbor == ordinal
                        || previous_neighbor.is_some_and(|previous| previous >= neighbor)
                    {
                        return Err(corrupt("DiskANN record has an invalid neighbor"));
                    }
                    previous_neighbor = Some(neighbor);
                } else if neighbor != u32::MAX {
                    return Err(corrupt("DiskANN neighbor padding is not canonical"));
                }
            }
        }
        Ok(())
    }
}

impl<R> DiskAnnRead for FileDiskAnn<R> {
    fn len(&self) -> usize {
        self.metadata.node_count
    }
    fn dimension(&self) -> usize {
        self.metadata.dimension
    }
    fn metric(&self) -> VectorDistance {
        self.metadata.metric
    }
    fn max_degree(&self) -> usize {
        self.metadata.max_degree
    }
    fn entry_point(&self) -> Option<u32> {
        self.metadata.entry_point
    }
    fn ids_are_sorted(&self) -> bool {
        true
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        read_u64(self.record(ordinal)?, 0)
    }
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>> {
        let record = self.record(ordinal)?;
        let count = read_u32(record, 16)? as usize;
        let start = RECORD_PREFIX_BYTES + self.dimension() * 4;
        let bytes = record
            .get(start..start + count * 4)
            .ok_or_else(|| corrupt("DiskANN neighbor range exceeds record"))?;
        Ok(bytes
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().expect("fixed-width neighbor"))))
    }
    fn distance(
        &self,
        query: &[f32],
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64> {
        checkpoint(task)?;
        let vector = self.vector(ordinal, task)?;
        with_distance_work(task, |checkpoint| {
            self.metric()
                .distance_slice_to_vector_with_work(query, vector, checkpoint)
        })
    }
    fn distance_prepared(
        &self,
        query: &hawdb_core::vector::PreparedVectorQuery<'_>,
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64> {
        checkpoint(task)?;
        let vector = self.vector(ordinal, task)?;
        with_distance_work(task, |checkpoint| {
            query.distance_to_vector_with_work(self.metric(), vector, checkpoint)
        })
    }
}

impl DiskAnnArtifactMetadata {
    fn record_bytes(&self) -> Result<usize> {
        self.dimension
            .checked_add(self.max_degree)
            .and_then(|words| words.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(RECORD_PREFIX_BYTES))
            .ok_or_else(|| corrupt("DiskANN record layout overflow"))
    }
    fn file_bytes(&self) -> Result<usize> {
        self.record_bytes()?
            .checked_mul(self.node_count)
            .and_then(|bytes| bytes.checked_add(HEADER_BYTES + FOOTER_BYTES))
            .ok_or_else(|| corrupt("DiskANN artifact layout overflow"))
    }
    fn encode(&self) -> Result<[u8; HEADER_BYTES]> {
        let mut bytes = [0; HEADER_BYTES];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..12].copy_from_slice(&u32_value(self.dimension)?.to_le_bytes());
        bytes[12] = match self.metric {
            VectorDistance::Cosine => 0,
            VectorDistance::Euclidean => 1,
        };
        bytes[16..20].copy_from_slice(&u32_value(self.max_degree)?.to_le_bytes());
        bytes[20..24].copy_from_slice(&u32_value(self.build_beam)?.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.alpha.to_le_bytes());
        if let Some(lsg) = self.lsg {
            bytes[32..36].copy_from_slice(&u32_value(lsg.sample_size)?.to_le_bytes());
            bytes[36..40].copy_from_slice(&u32_value(lsg.neighbors)?.to_le_bytes());
            bytes[40..48].copy_from_slice(&lsg.exponent.to_le_bytes());
        }
        bytes[48..56].copy_from_slice(&(self.node_count as u64).to_le_bytes());
        bytes[56..60].copy_from_slice(&self.entry_point.unwrap_or(u32::MAX).to_le_bytes());
        bytes[64..96].copy_from_slice(&self.identity.source_digest);
        bytes[96..104].copy_from_slice(&self.identity.source_epoch.to_le_bytes());
        bytes[104..112].copy_from_slice(&self.identity.generation.to_le_bytes());
        bytes[112..144].copy_from_slice(&self.identity.binding_digest);
        Ok(bytes)
    }
    fn decode(bytes: &[u8; HEADER_BYTES]) -> Result<Self> {
        if &bytes[..8] != MAGIC || bytes[13..16] != [0; 3] || bytes[60..64] != [0; 4] {
            return Err(corrupt("unsupported DiskANN header"));
        }
        let lsg = if read_u32(bytes, 32)? == 0 {
            if bytes[32..48] != [0; 16] {
                return Err(corrupt("noncanonical disabled LSG settings"));
            }
            None
        } else {
            Some(LsgBuildConfig {
                sample_size: read_u32(bytes, 32)? as usize,
                neighbors: read_u32(bytes, 36)? as usize,
                exponent: read_f64(bytes, 40)?,
            })
        };
        let node_count = read_u64(bytes, 48)?;
        if node_count > u32::MAX as u64 {
            return Err(corrupt("DiskANN count exceeds ordinal space"));
        }
        let entry = read_u32(bytes, 56)?;
        if (node_count == 0 && entry != u32::MAX)
            || (node_count != 0 && u64::from(entry) >= node_count)
        {
            return Err(corrupt("invalid DiskANN entry point"));
        }
        let metadata = Self {
            identity: DiskAnnArtifactIdentity {
                source_digest: bytes[64..96].try_into().expect("fixed source digest"),
                binding_digest: bytes[112..144].try_into().expect("fixed binding digest"),
                source_epoch: read_u64(bytes, 96)?,
                generation: read_u64(bytes, 104)?,
            },
            dimension: read_u32(bytes, 8)? as usize,
            metric: match bytes[12] {
                0 => VectorDistance::Cosine,
                1 => VectorDistance::Euclidean,
                _ => return Err(corrupt("unsupported DiskANN metric")),
            },
            node_count: node_count as usize,
            max_degree: read_u32(bytes, 16)? as usize,
            build_beam: read_u32(bytes, 20)? as usize,
            alpha: read_f64(bytes, 24)?,
            lsg,
            entry_point: (node_count != 0).then_some(entry),
        };
        build::validate_config(
            metadata.dimension,
            DiskAnnBuildConfig {
                max_degree: metadata.max_degree,
                build_beam: metadata.build_beam,
                alpha: metadata.alpha,
                lsg,
                max_working_bytes: 0,
            },
        )
        .map_err(|_| corrupt("invalid DiskANN construction settings"))?;
        metadata.file_bytes()?;
        Ok(metadata)
    }
}

fn u32_value(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| configuration("DiskANN artifact parameter exceeds u32"))
}
fn read_u32(bytes: &[u8], start: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(start..start + 4)
            .ok_or_else(|| corrupt("truncated DiskANN field"))?
            .try_into()
            .expect("fixed u32 field"),
    ))
}
fn read_u64(bytes: &[u8], start: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(start..start + 8)
            .ok_or_else(|| corrupt("truncated DiskANN field"))?
            .try_into()
            .expect("fixed u64 field"),
    ))
}
fn read_f64(bytes: &[u8], start: usize) -> Result<f64> {
    Ok(f64::from_bits(read_u64(bytes, start)?))
}
