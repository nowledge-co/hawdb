use super::ROOT_DESCRIPTOR_BINDING_OFFSET;
use crate::relational::row_page::publication::{
    durability, RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPageRootDescriptor, RelationalRowPageRootManifest, RelationalRowPageSlotIntegrity,
};
use crate::relational::RelationalRowPageId;
use skein_integrity::{IntegrityHasher, Sha256Digest};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroU64;

pub(in crate::relational::row_page::publication) const ROOT_DESCRIPTOR_BYTES: usize = 136;

pub(in crate::relational::row_page::publication) fn read_descriptor(
    descriptors: &mut File,
    keys: &mut File,
    ordinal: u64,
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
) -> Result<RelationalRowPageRootDescriptor, RelationalRowPagePublicationError> {
    if ordinal >= manifest.root_page_count {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor ordinal {ordinal} exceeds root page count {}",
            manifest.root_page_count
        )));
    }
    let descriptor_offset = ordinal
        .checked_mul(ROOT_DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page descriptor offset overflow".to_string(),
            )
        })?;
    descriptors
        .seek(SeekFrom::Start(descriptor_offset))
        .map_err(durability("seek row-page root descriptor"))?;
    let mut encoded = [0u8; ROOT_DESCRIPTOR_BYTES];
    descriptors
        .read_exact(&mut encoded)
        .map_err(durability("read row-page root descriptor"))?;
    let wire = WireDescriptor::decode(&encoded);
    let lower_bound = read_key(
        keys,
        wire.lower_offset,
        wire.lower_len,
        manifest.root_key_artifact.encoded_len,
        config,
        "lower",
    )?;
    let upper_bound = read_key(
        keys,
        wire.upper_offset,
        wire.upper_len,
        manifest.root_key_artifact.encoded_len,
        config,
        "upper",
    )?;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&manifest.generation.to_le_bytes());
    hasher.update(&ordinal.to_le_bytes());
    hasher.update(&encoded[..ROOT_DESCRIPTOR_BINDING_OFFSET]);
    hasher.update(&lower_bound);
    hasher.update(&upper_bound);
    let digest = hasher.finish();
    if digest.crc32c.get() != wire.binding_crc32c || digest.sha256 != wire.binding_sha256 {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page root descriptor binding checksum mismatch".to_string(),
        ));
    }
    let descriptor = RelationalRowPageRootDescriptor {
        logical_page_id: RelationalRowPageId::new(
            NonZeroU64::new(wire.logical_page_id).ok_or_else(|| {
                RelationalRowPagePublicationError::Corrupt(
                    "row-page descriptor has a zero logical page id".to_string(),
                )
            })?,
        ),
        physical_generation: wire.physical_generation,
        physical_slot: wire.physical_slot,
        source_commit_epoch: wire.source_commit_epoch,
        row_count: wire.row_count,
        lower_bound,
        upper_bound,
        slot_integrity: RelationalRowPageSlotIntegrity {
            encoded_len: wire.encoded_len,
            slot_crc32c: wire.slot_crc32c,
            slot_sha256: wire.slot_sha256,
        },
    };
    validate_descriptor(
        &descriptor,
        manifest.generation,
        manifest.source_commit_epoch,
        config,
    )?;
    let index = manifest
        .physical_generations
        .binary_search_by_key(&descriptor.physical_generation, |entry| entry.generation)
        .map_err(|_| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page descriptor references an unaccounted physical generation".to_string(),
            )
        })?;
    let allocated_pages = manifest.physical_generations[index].allocated_pages;
    if descriptor.physical_slot >= allocated_pages {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor references slot {} outside generation {} allocation {}",
            descriptor.physical_slot, descriptor.physical_generation, allocated_pages
        )));
    }
    Ok(descriptor)
}

fn read_key(
    file: &mut File,
    offset: u64,
    len: u32,
    artifact_len: u64,
    config: RelationalRowPagePublicationConfig,
    bound: &str,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let len = len as usize;
    if len == 0 || len > config.page_limits.max_key_bytes.get() {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page {bound} key contains {len} bytes outside the admitted range"
        )));
    }
    let end = offset.checked_add(len as u64).ok_or_else(|| {
        RelationalRowPagePublicationError::Corrupt(format!("row-page {bound} key offset overflow"))
    })?;
    if end > artifact_len {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page {bound} key exceeds its artifact"
        )));
    }
    let mut bytes = vec![0; len];
    file.seek(SeekFrom::Start(offset))
        .map_err(durability("seek row-page root key"))?;
    file.read_exact(&mut bytes)
        .map_err(durability("read row-page root key"))?;
    Ok(bytes)
}

pub(super) fn validate_descriptor(
    descriptor: &RelationalRowPageRootDescriptor,
    root_generation: u64,
    root_epoch: u64,
    config: RelationalRowPagePublicationConfig,
) -> Result<(), RelationalRowPagePublicationError> {
    if descriptor.physical_generation == 0 || descriptor.physical_generation > root_generation {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor physical generation {} is outside 1..={root_generation}",
            descriptor.physical_generation
        )));
    }
    if descriptor.source_commit_epoch == 0 || descriptor.source_commit_epoch > root_epoch {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor source epoch {} is outside 1..={root_epoch}",
            descriptor.source_commit_epoch
        )));
    }
    if descriptor.row_count == 0
        || descriptor.row_count as usize > config.page_limits.max_rows.get()
    {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor row count {} is outside the admitted range",
            descriptor.row_count
        )));
    }
    if descriptor.slot_integrity.encoded_len == 0
        || descriptor.slot_integrity.encoded_len as usize > config.page_limits.max_page_bytes.get()
    {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor encoded length {} is outside the admitted range",
            descriptor.slot_integrity.encoded_len
        )));
    }
    if descriptor.lower_bound.is_empty()
        || descriptor.upper_bound.is_empty()
        || descriptor.lower_bound > descriptor.upper_bound
        || descriptor.lower_bound.len() > config.page_limits.max_key_bytes.get()
        || descriptor.upper_bound.len() > config.page_limits.max_key_bytes.get()
    {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page descriptor has invalid key bounds".to_string(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct WireDescriptor {
    pub logical_page_id: u64,
    pub physical_generation: u64,
    pub physical_slot: u64,
    pub source_commit_epoch: u64,
    pub row_count: u32,
    pub encoded_len: u32,
    pub lower_offset: u64,
    pub lower_len: u32,
    pub upper_offset: u64,
    pub upper_len: u32,
    pub slot_crc32c: u32,
    pub slot_sha256: Sha256Digest,
    pub binding_crc32c: u32,
    pub binding_sha256: Sha256Digest,
}

impl WireDescriptor {
    pub(super) fn encode(self) -> [u8; ROOT_DESCRIPTOR_BYTES] {
        let mut encoded = [0u8; ROOT_DESCRIPTOR_BYTES];
        encoded[0..8].copy_from_slice(&self.logical_page_id.to_le_bytes());
        encoded[8..16].copy_from_slice(&self.physical_generation.to_le_bytes());
        encoded[16..24].copy_from_slice(&self.physical_slot.to_le_bytes());
        encoded[24..32].copy_from_slice(&self.source_commit_epoch.to_le_bytes());
        encoded[32..36].copy_from_slice(&self.row_count.to_le_bytes());
        encoded[36..40].copy_from_slice(&self.encoded_len.to_le_bytes());
        encoded[40..48].copy_from_slice(&self.lower_offset.to_le_bytes());
        encoded[48..52].copy_from_slice(&self.lower_len.to_le_bytes());
        encoded[52..60].copy_from_slice(&self.upper_offset.to_le_bytes());
        encoded[60..64].copy_from_slice(&self.upper_len.to_le_bytes());
        encoded[64..68].copy_from_slice(&self.slot_crc32c.to_le_bytes());
        encoded[68..100].copy_from_slice(self.slot_sha256.as_bytes());
        encoded[100..104].copy_from_slice(&self.binding_crc32c.to_le_bytes());
        encoded[104..136].copy_from_slice(self.binding_sha256.as_bytes());
        encoded
    }

    fn decode(encoded: &[u8; ROOT_DESCRIPTOR_BYTES]) -> Self {
        Self {
            logical_page_id: read_u64(&encoded[0..8]),
            physical_generation: read_u64(&encoded[8..16]),
            physical_slot: read_u64(&encoded[16..24]),
            source_commit_epoch: read_u64(&encoded[24..32]),
            row_count: read_u32(&encoded[32..36]),
            encoded_len: read_u32(&encoded[36..40]),
            lower_offset: read_u64(&encoded[40..48]),
            lower_len: read_u32(&encoded[48..52]),
            upper_offset: read_u64(&encoded[52..60]),
            upper_len: read_u32(&encoded[60..64]),
            slot_crc32c: read_u32(&encoded[64..68]),
            slot_sha256: Sha256Digest::from_bytes(
                encoded[68..100]
                    .try_into()
                    .expect("slot digest length was checked"),
            ),
            binding_crc32c: read_u32(&encoded[100..104]),
            binding_sha256: Sha256Digest::from_bytes(
                encoded[104..136]
                    .try_into()
                    .expect("binding digest length was checked"),
            ),
        }
    }
}

fn read_u32(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(encoded.try_into().expect("u32 field has a fixed length"))
}

fn read_u64(encoded: &[u8]) -> u64 {
    u64::from_le_bytes(encoded.try_into().expect("u64 field has a fixed length"))
}
