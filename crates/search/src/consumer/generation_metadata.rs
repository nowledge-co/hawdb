// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Immutable Consumer generation metadata. A decoded description is not a
//! publication, source authority, completed receipt or serving capability.
//!
//! This codec retains full producer-supplied object references; missing bindings
//! never authorize an inode/length fallback. Its borrowed offset index permits
//! logarithmic lookup without copying the complete artifact map into a reader.

use super::ConsumerBinding;
use crate::error::{HawDBError, Result};
use hawdb_core::{RuntimeTaskContext, Uuid};
use hawdb_integrity::{IntegrityHasher, Sha256Digest};
use hawdb_storage::immutable_object::{ObjectKind, ObjectReference};
use std::io::{self, Write};
use std::num::NonZeroU64;

const MAGIC: &[u8; 8] = b"HAWCGM02";
const REFERENCE_BYTES: usize = 1 + 2 + 8 + 32;
const CHECKSUM_BYTES: usize = 32;
const HEADER_BYTES: usize = 8 + 4 * 16 + 2 + 4 * 8 + 16 + REFERENCE_BYTES + 4;

/// Operation-supplied bounds; these do not create a separate resource allowance.
#[derive(Debug, Clone, Copy)]
pub struct MetadataLimits {
    pub encoded_bytes: usize,
    pub artifacts: usize,
    pub artifact_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ArtifactReference<'a> {
    pub name: &'a str,
    pub reference: ObjectReference,
}

/// The producer seals these references before preparing a Consumer selector.
/// Counter fields describe, but do not mint or prove, non-reused incarnations.
#[derive(Debug)]
pub struct MetadataInput<'a> {
    pub binding: &'a ConsumerBinding,
    pub source_epoch: u64,
    pub target_incarnation: NonZeroU64,
    pub generation: NonZeroU64,
    pub operation: NonZeroU64,
    pub component_uuid: Uuid,
    pub search_manifest: ObjectReference,
    pub artifacts: &'a [ArtifactReference<'a>],
}

/// A zero-copy validated description. The input's owner must retain its actual
/// memory admission and establish producer/receipt authority separately.
#[derive(Debug, Clone, Copy)]
pub struct MetadataView<'a> {
    database_uuid: Uuid,
    projection_uuid: Uuid,
    registration_uuid: Uuid,
    checkpoint_uuid: Uuid,
    consumer_id: &'a str,
    source_epoch: u64,
    target_incarnation: NonZeroU64,
    generation: NonZeroU64,
    operation: NonZeroU64,
    component_uuid: Uuid,
    search_manifest: ObjectReference,
    offsets: &'a [u8],
    records: &'a [u8],
    artifact_count: usize,
    limits: MetadataLimits,
}

impl MetadataInput<'_> {
    /// Validate and size the entire description before writing any bytes.
    /// Callers reserve this capacity before allocating a destination buffer.
    pub fn encoded_len(&self, limits: MetadataLimits, task: &RuntimeTaskContext) -> Result<usize> {
        crate::build_control::checkpoint(task)?;
        if !super::valid_consumer_id(&self.binding.consumer_id)
            || self.component_uuid.is_nil()
            || u32::try_from(self.artifacts.len()).is_err()
        {
            return Err(invalid());
        }
        if self.artifacts.len() > limits.artifacts {
            return Err(limit_exceeded());
        }
        validate_reference(self.search_manifest, limits)?;
        let mut records = 0usize;
        let mut previous = None;
        for artifact in self.artifacts {
            crate::build_control::checkpoint(task)?;
            validate_name(artifact.name)?;
            validate_reference(artifact.reference, limits)?;
            if previous.is_some_and(|name| name >= artifact.name) {
                return Err(invalid());
            }
            previous = Some(artifact.name);
            records = records
                .checked_add(2 + REFERENCE_BYTES)
                .and_then(|n| n.checked_add(artifact.name.len()))
                .ok_or_else(invalid)?;
        }
        let index = self
            .artifacts
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(invalid)?;
        let length = HEADER_BYTES
            .checked_add(self.binding.consumer_id.len())
            .and_then(|n| n.checked_add(index))
            .and_then(|n| n.checked_add(records))
            .and_then(|n| n.checked_add(CHECKSUM_BYTES))
            .ok_or_else(invalid)?;
        if u32::try_from(length).is_err() {
            return Err(invalid());
        }
        if length > limits.encoded_bytes {
            return Err(limit_exceeded());
        }
        Ok(length)
    }

    /// Streaming serialization only. This performs no file open, flush, sync,
    /// namespace mutation or acknowledgement. Failed writes may leave a prefix
    /// in the caller-owned private candidate, whose cleanup custody must remain.
    pub fn encode_to(
        &self,
        output: &mut impl Write,
        limits: MetadataLimits,
        task: &RuntimeTaskContext,
    ) -> Result<usize> {
        let length = self.encoded_len(limits, task)?;
        let mut writer = HashedWriter {
            output,
            hasher: IntegrityHasher::new(),
        };
        writer.write_all(MAGIC)?;
        for uuid in [
            self.binding.database_uuid,
            self.binding.projection_uuid,
            self.binding.registration_uuid,
            self.binding.checkpoint_uuid,
        ] {
            writer.write_all(uuid.as_bytes())?;
        }
        writer.write_all(&(self.binding.consumer_id.len() as u16).to_le_bytes())?;
        writer.write_all(self.binding.consumer_id.as_bytes())?;
        for value in [
            self.source_epoch,
            self.target_incarnation.get(),
            self.generation.get(),
            self.operation.get(),
        ] {
            writer.write_all(&value.to_le_bytes())?;
        }
        writer.write_all(self.component_uuid.as_bytes())?;
        write_reference(&mut writer, self.search_manifest)?;
        writer.write_all(&(self.artifacts.len() as u32).to_le_bytes())?;
        let mut offset = 0u32;
        writer.write_all(&offset.to_le_bytes())?;
        for artifact in self.artifacts {
            crate::build_control::checkpoint(task)?;
            // encoded_len has already checked the complete u32-sized frame.
            offset += (2 + artifact.name.len() + REFERENCE_BYTES) as u32;
            writer.write_all(&offset.to_le_bytes())?;
        }
        for artifact in self.artifacts {
            crate::build_control::checkpoint(task)?;
            writer.write_all(&(artifact.name.len() as u16).to_le_bytes())?;
            writer.write_all(artifact.name.as_bytes())?;
            write_reference(&mut writer, artifact.reference)?;
        }
        crate::build_control::checkpoint(task)?;
        let checksum = writer.hasher.finish().sha256;
        writer.output.write_all(checksum.as_bytes())?;
        // A late cancellation can follow a complete private write. Report it
        // without deleting or treating the caller-owned bytes as rolled back.
        crate::build_control::checkpoint(task)?;
        Ok(length)
    }
}

impl<'a> MetadataView<'a> {
    /// Checks the complete bounded frame and every sorted index/record before
    /// exposing a view. No decoded collection or filename allocation occurs.
    pub fn decode(
        bytes: &'a [u8],
        limits: MetadataLimits,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        crate::build_control::checkpoint(task)?;
        if bytes.len() > limits.encoded_bytes {
            return Err(limit_exceeded());
        }
        if u32::try_from(bytes.len()).is_err() {
            return Err(invalid());
        }
        let split = bytes
            .len()
            .checked_sub(CHECKSUM_BYTES)
            .ok_or_else(invalid)?;
        let (body, checksum) = bytes.split_at(split);
        let mut hasher = IntegrityHasher::new();
        for chunk in body.chunks(64 << 10) {
            crate::build_control::checkpoint(task)?;
            hasher.update(chunk);
        }
        if hasher.finish().sha256.as_bytes() != checksum {
            return Err(invalid());
        }
        let mut reader = Cursor(body);
        if reader.take(8)? != MAGIC {
            return Err(invalid());
        }
        let database_uuid = reader.uuid()?;
        let projection_uuid = reader.uuid()?;
        let registration_uuid = reader.uuid()?;
        let checkpoint_uuid = reader.uuid()?;
        let consumer_id = reader.name()?;
        if !super::valid_consumer_id(consumer_id) {
            return Err(invalid());
        }
        let source_epoch = reader.u64()?;
        let target_incarnation = NonZeroU64::new(reader.u64()?).ok_or_else(invalid)?;
        let generation = NonZeroU64::new(reader.u64()?).ok_or_else(invalid)?;
        let operation = NonZeroU64::new(reader.u64()?).ok_or_else(invalid)?;
        let component_uuid = reader.uuid()?;
        if component_uuid.is_nil() {
            return Err(invalid());
        }
        let search_manifest = reader.reference(limits)?;
        let artifact_count = reader.u32()? as usize;
        if artifact_count > limits.artifacts {
            return Err(limit_exceeded());
        }
        let index_len = artifact_count
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(invalid)?;
        let offsets = reader.take(index_len)?;
        let view = Self {
            database_uuid,
            projection_uuid,
            registration_uuid,
            checkpoint_uuid,
            consumer_id,
            source_epoch,
            target_incarnation,
            generation,
            operation,
            component_uuid,
            search_manifest,
            offsets,
            records: reader.0,
            artifact_count,
            limits,
        };
        if view.offset(0)? != 0 || view.offset(artifact_count)? != view.records.len() {
            return Err(invalid());
        }
        let mut previous = None;
        for index in 0..artifact_count {
            crate::build_control::checkpoint(task)?;
            let artifact = view.entry(index, limits)?;
            if previous.is_some_and(|name| name >= artifact.name) {
                return Err(invalid());
            }
            previous = Some(artifact.name);
        }
        Ok(view)
    }

    pub fn source_epoch(&self) -> u64 {
        self.source_epoch
    }
    pub fn target_incarnation(&self) -> NonZeroU64 {
        self.target_incarnation
    }
    pub fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    pub fn operation(&self) -> NonZeroU64 {
        self.operation
    }
    pub fn component_uuid(&self) -> Uuid {
        self.component_uuid
    }
    pub fn search_manifest(&self) -> ObjectReference {
        self.search_manifest
    }

    pub fn matches_binding(&self, binding: &ConsumerBinding) -> bool {
        self.database_uuid == binding.database_uuid
            && self.projection_uuid == binding.projection_uuid
            && self.registration_uuid == binding.registration_uuid
            && self.checkpoint_uuid == binding.checkpoint_uuid
            && self.consumer_id == binding.consumer_id
    }

    pub fn artifact_count(&self) -> usize {
        self.artifact_count
    }

    /// Borrow the complete validated artifact map without copying names.
    pub fn artifacts(&self) -> impl ExactSizeIterator<Item = ArtifactReference<'a>> + '_ {
        (0..self.artifact_count).map(|index| {
            self.entry(index, self.limits)
                .expect("validated immutable artifact index")
        })
    }

    /// Exact lookup in the validated borrowed index; no path or mutable global
    /// alias map participates. An absent entry stays absent.
    pub fn artifact(&self, name: &str) -> Option<ObjectReference> {
        let mut low = 0;
        let mut high = self.artifact_count;
        while low < high {
            let middle = low + (high - low) / 2;
            let artifact = self
                .entry(middle, self.limits)
                .expect("immutable metadata view has a validated index");
            match artifact.name.cmp(name) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Some(artifact.reference),
            }
        }
        None
    }

    fn offset(&self, index: usize) -> Result<usize> {
        let start = index.checked_mul(4).ok_or_else(invalid)?;
        let end = start.checked_add(4).ok_or_else(invalid)?;
        let bytes = self.offsets.get(start..end).ok_or_else(invalid)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| invalid())?) as usize)
    }

    fn entry(&self, index: usize, limits: MetadataLimits) -> Result<ArtifactReference<'a>> {
        let start = self.offset(index)?;
        let end = self.offset(index + 1)?;
        let mut reader = Cursor(self.records.get(start..end).ok_or_else(invalid)?);
        let name = reader.name()?;
        validate_name(name)?;
        let reference = reader.reference(limits)?;
        if !reader.0.is_empty() {
            return Err(invalid());
        }
        Ok(ArtifactReference { name, reference })
    }
}

fn validate_name(name: &str) -> Result<()> {
    if u16::try_from(name.len()).is_err()
        || (name != crate::SEARCH_SNAPSHOT_FILE
            && crate::out_of_core::artifact_key::ArtifactKey::from_file_name(name).is_none())
    {
        return Err(invalid());
    }
    Ok(())
}
fn validate_reference(reference: ObjectReference, limits: MetadataLimits) -> Result<()> {
    if reference.kind != ObjectKind::CheckpointArtifact || reference.format_version != 1 {
        return Err(invalid());
    }
    if reference.byte_length > limits.artifact_bytes {
        return Err(limit_exceeded());
    }
    Ok(())
}
fn limit_exceeded() -> HawDBError {
    HawDBError::Execution("Consumer generation metadata exceeds its operation limit".into())
}
fn invalid() -> HawDBError {
    HawDBError::StorageIntegrity("invalid Consumer generation metadata".into())
}
fn write_reference(writer: &mut impl Write, reference: ObjectReference) -> io::Result<()> {
    writer.write_all(&[reference.kind as u8])?;
    writer.write_all(&reference.format_version.to_le_bytes())?;
    writer.write_all(&reference.byte_length.to_le_bytes())?;
    writer.write_all(reference.sha256.as_bytes())
}
struct HashedWriter<'a, W> {
    output: &'a mut W,
    hasher: IntegrityHasher,
}
impl<W: Write> Write for HashedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.output.write(bytes)?;
        self.hasher.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let value = self.0.get(..count).ok_or_else(invalid)?;
        self.0 = &self.0[count..];
        Ok(value)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn uuid(&mut self) -> Result<Uuid> {
        Ok(Uuid::from_bytes(
            self.take(16)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn name(&mut self) -> Result<&'a str> {
        let length = self.u16()? as usize;
        std::str::from_utf8(self.take(length)?).map_err(|_| invalid())
    }
    fn reference(&mut self, limits: MetadataLimits) -> Result<ObjectReference> {
        if self.take(1)? != [ObjectKind::CheckpointArtifact as u8] {
            return Err(invalid());
        }
        let reference = ObjectReference {
            kind: ObjectKind::CheckpointArtifact,
            format_version: self.u16()?,
            byte_length: self.u64()?,
            sha256: Sha256Digest::from_bytes(self.take(32)?.try_into().map_err(|_| invalid())?),
        };
        validate_reference(reference, limits)?;
        Ok(reference)
    }
}

#[cfg(test)]
mod tests;
