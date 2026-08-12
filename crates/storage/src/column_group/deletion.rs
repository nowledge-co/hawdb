//! Deletion vector sidecar (§3.3, §3.5.3(d)).
//!
//! A deletion vector marks rows of one published node group as deleted or
//! superseded without rewriting the group. It is bound to
//! `(group id, group generation, publication generation)`. The group
//! generation fixes the row-ordinal space; the publication generation fixes
//! when the cumulative bitmap first became visible. A newer checkpoint may
//! therefore publish deletes against an older immutable group without
//! rewriting that group's bytes, while an older snapshot rejects a bitmap
//! published in its future.
//!
//! Representation: a plain fixed bitmap, one bit per row. Groups hold at
//! most 65,536 rows by default, so the bitmap tops out at 8 KiB — smaller
//! and simpler than a `RoaringTreemap` (which earns its keep on sparse u64
//! record-id spaces, not on dense u32 row offsets), with O(1) mark/test and
//! a trivially fixed serialized layout.
//!
//! File layout: `SKNCOLDV1 | body | u64 body length | u32 crc32c(body) |
//! SKNCOLDV1`, published with the temp-file, fsync, rename protocol.

use super::encoding::Cursor;
use super::{corrupt, unsupported, ColumnGroupError, DeletionVectorBinding, DELETION_VECTOR_MAGIC};
use crate::durability::durable_replace_file;
use crate::ManifestGeneration;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const DELETION_VECTOR_VERSION: u32 = 1;
const FOOTER_BYTES: usize = 8 + 4 + DELETION_VECTOR_MAGIC.len();

pub(super) fn encoded_file_len(row_count: u32) -> u64 {
    (DELETION_VECTOR_MAGIC.len() + FOOTER_BYTES + 40 + word_count(row_count) * 8) as u64
}

/// A generation-scoped bitmap of deleted rows in one node group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVector {
    binding: DeletionVectorBinding,
    deleted_count: u32,
    words: Vec<u64>,
}

impl DeletionVector {
    /// An empty cumulative vector bound to one immutable group and one
    /// publication generation.
    pub fn new(binding: DeletionVectorBinding) -> Self {
        Self {
            binding,
            deleted_count: 0,
            words: vec![0; word_count(binding.row_count)],
        }
    }

    pub fn binding(&self) -> DeletionVectorBinding {
        self.binding
    }

    pub fn group_id(&self) -> u64 {
        self.binding.group_id
    }

    pub fn group_generation(&self) -> ManifestGeneration {
        self.binding.group_generation
    }

    pub fn publication_generation(&self) -> ManifestGeneration {
        self.binding.publication_generation
    }

    pub fn row_count(&self) -> u32 {
        self.binding.row_count
    }

    pub fn deleted_count(&self) -> u32 {
        self.deleted_count
    }

    /// Live rows remaining, answering cardinality questions from metadata
    /// alone (§3.2.4).
    pub fn visible_count(&self) -> u32 {
        self.binding.row_count - self.deleted_count
    }

    /// Carries a cumulative bitmap into a later manifest generation without
    /// changing the physical group or row ordinals it addresses.
    pub fn fork_for_publication(
        &self,
        publication_generation: ManifestGeneration,
    ) -> Result<Self, ColumnGroupError> {
        if publication_generation.0 <= self.binding.publication_generation.0 {
            return Err(unsupported(format!(
                "deletion vector publication generation {} must advance beyond {}",
                publication_generation.0, self.binding.publication_generation.0
            )));
        }
        let mut next = self.clone();
        next.binding.publication_generation = publication_generation;
        Ok(next)
    }

    /// Marks a row deleted; returns whether the row was newly marked.
    pub fn mark_deleted(&mut self, row_index: u32) -> Result<bool, ColumnGroupError> {
        if row_index >= self.binding.row_count {
            return Err(ColumnGroupError::RowOutOfRange {
                row_index,
                row_count: self.binding.row_count,
            });
        }
        let word = &mut self.words[row_index as usize / 64];
        let bit = 1u64 << (row_index % 64);
        if *word & bit != 0 {
            return Ok(false);
        }
        *word |= bit;
        self.deleted_count += 1;
        Ok(true)
    }

    pub fn is_deleted(&self, row_index: u32) -> bool {
        row_index < self.binding.row_count
            && self.words[row_index as usize / 64] & (1u64 << (row_index % 64)) != 0
    }

    /// Row indices still visible under this vector, ascending.
    pub fn visible_rows(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.binding.row_count).filter(|row| !self.is_deleted(*row))
    }

    /// Row indices marked deleted, ascending.
    pub fn deleted_rows(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.binding.row_count).filter(|row| self.is_deleted(*row))
    }

    /// Serializes and publishes the sidecar with temp file, fsync, rename.
    pub fn write(&self, path: &Path) -> Result<(), ColumnGroupError> {
        let tmp_path = path.with_extension("skein.tmp");
        let result = self.write_inner(&tmp_path);
        if let Err(error) = result {
            let _ = fs::remove_file(&tmp_path);
            return Err(error);
        }
        durable_replace_file(&tmp_path, path)?;
        Ok(())
    }

    fn write_inner(&self, path: &Path) -> Result<(), ColumnGroupError> {
        let body = self.encode_body();
        let mut file = File::create(path)?;
        file.write_all(DELETION_VECTOR_MAGIC)?;
        file.write_all(&body)?;
        file.write_all(&(body.len() as u64).to_le_bytes())?;
        file.write_all(&skein_integrity::crc32c(&body).get().to_le_bytes())?;
        file.write_all(DELETION_VECTOR_MAGIC)?;
        file.sync_all()?;
        Ok(())
    }

    fn encode_body(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(40 + self.words.len() * 8);
        body.extend(DELETION_VECTOR_VERSION.to_le_bytes());
        body.extend(self.binding.row_count.to_le_bytes());
        body.extend(self.binding.group_id.to_le_bytes());
        body.extend(self.binding.group_generation.0.to_le_bytes());
        body.extend(self.binding.publication_generation.0.to_le_bytes());
        body.extend(self.deleted_count.to_le_bytes());
        body.extend((self.words.len() as u32).to_le_bytes());
        for word in &self.words {
            body.extend(word.to_le_bytes());
        }
        body
    }

    /// Opens a sidecar, validating the checksummed footer and every
    /// structural invariant of the bitmap.
    pub fn open(path: &Path) -> Result<Self, ColumnGroupError> {
        let bytes = fs::read(path)?;
        let minimum = DELETION_VECTOR_MAGIC.len() + FOOTER_BYTES;
        if bytes.len() < minimum {
            return Err(corrupt(format!(
                "deletion vector holds {} bytes, below the {minimum} byte minimum",
                bytes.len()
            )));
        }
        if &bytes[..DELETION_VECTOR_MAGIC.len()] != DELETION_VECTOR_MAGIC {
            return Err(corrupt(
                "deletion vector header magic is invalid".to_string(),
            ));
        }
        let footer_start = bytes.len() - FOOTER_BYTES;
        let footer = &bytes[footer_start..];
        if &footer[12..] != DELETION_VECTOR_MAGIC {
            return Err(corrupt(
                "deletion vector footer magic is invalid".to_string(),
            ));
        }
        let body_len = u64::from_le_bytes(footer[..8].try_into().expect("8B"));
        let stored_crc = u32::from_le_bytes(footer[8..12].try_into().expect("4B"));
        let body_start = DELETION_VECTOR_MAGIC.len() as u64;
        if body_len != footer_start as u64 - body_start {
            return Err(corrupt(format!(
                "deletion vector body length {body_len} does not match the file"
            )));
        }
        let body = &bytes[body_start as usize..footer_start];
        if skein_integrity::crc32c(body).get() != stored_crc {
            return Err(corrupt(
                "deletion vector checksum does not match its contents".to_string(),
            ));
        }
        Self::decode_body(body)
    }

    fn decode_body(body: &[u8]) -> Result<Self, ColumnGroupError> {
        let mut cursor = Cursor::new(body);
        let version = cursor.read_u32("deletion vector version")?;
        if version != DELETION_VECTOR_VERSION {
            return Err(corrupt(format!(
                "unsupported deletion vector version {version}"
            )));
        }
        let row_count = cursor.read_u32("deletion vector row count")?;
        let group_id = cursor.read_u64("deletion vector group id")?;
        let group_generation =
            ManifestGeneration(cursor.read_u64("deletion vector group generation")?);
        let publication_generation =
            ManifestGeneration(cursor.read_u64("deletion vector publication generation")?);
        let deleted_count = cursor.read_u32("deletion vector deleted count")?;
        let stored_words = cursor.read_u32("deletion vector word count")?;
        if stored_words as usize != word_count(row_count) {
            return Err(corrupt(format!(
                "deletion vector holds {stored_words} words for {row_count} rows"
            )));
        }
        let mut words = Vec::with_capacity(stored_words as usize);
        for _ in 0..stored_words {
            words.push(cursor.read_u64("deletion vector bitmap word")?);
        }
        cursor.expect_exhausted("deletion vector body")?;
        if row_count % 64 != 0
            && let Some(tail) = words.last()
            && tail & !((1u64 << (row_count % 64)) - 1) != 0
        {
            return Err(corrupt(
                "deletion vector sets bits beyond the row count".to_string(),
            ));
        }
        let set_bits = words.iter().map(|word| word.count_ones()).sum::<u32>();
        if set_bits != deleted_count {
            return Err(corrupt(format!(
                "deletion vector marks {set_bits} rows but declares {deleted_count}"
            )));
        }
        Ok(Self {
            binding: DeletionVectorBinding::new(
                group_id,
                group_generation,
                publication_generation,
                row_count,
            )
            .map_err(|error| corrupt(error.to_string()))?,
            deleted_count,
            words,
        })
    }
}

fn word_count(row_count: u32) -> usize {
    (row_count as usize).div_ceil(64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn unique_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-column-dv-{name}-{nonce}.skein"))
    }

    fn binding(
        group_id: u64,
        group_generation: u64,
        publication_generation: u64,
        row_count: u32,
    ) -> DeletionVectorBinding {
        DeletionVectorBinding::new(
            group_id,
            ManifestGeneration(group_generation),
            ManifestGeneration(publication_generation),
            row_count,
        )
        .unwrap()
    }

    #[test]
    fn deletion_vector_round_trips_and_merges_visibility() {
        let path = unique_path("round_trip");
        let mut vector = DeletionVector::new(binding(9, 2, 4, 130));
        for row in [0, 1, 63, 64, 129] {
            assert!(vector.mark_deleted(row).unwrap());
            assert!(!vector.mark_deleted(row).unwrap());
        }
        assert_eq!(vector.deleted_count(), 5);
        assert_eq!(vector.visible_count(), 125);
        vector.write(&path).unwrap();
        assert!(!path.with_extension("skein.tmp").exists());
        let reopened = DeletionVector::open(&path).unwrap();
        assert_eq!(reopened, vector);
        let visible = reopened.visible_rows().collect::<Vec<_>>();
        assert_eq!(visible.len(), 125);
        assert!(!visible.contains(&63));
        assert!(visible.contains(&62));
        assert_eq!(
            reopened.deleted_rows().collect::<Vec<_>>(),
            vec![0, 1, 63, 64, 129]
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn all_deleted_and_none_deleted_vectors_merge_correctly() {
        let path = unique_path("extremes");
        let mut all = DeletionVector::new(binding(1, 1, 1, 64));
        for row in 0..64 {
            all.mark_deleted(row).unwrap();
        }
        all.write(&path).unwrap();
        let all = DeletionVector::open(&path).unwrap();
        assert_eq!(all.visible_rows().count(), 0);
        assert_eq!(all.visible_count(), 0);
        let none = DeletionVector::new(binding(1, 1, 1, 64));
        none.write(&path).unwrap();
        let none = DeletionVector::open(&path).unwrap();
        assert_eq!(none.visible_rows().count(), 64);
        assert_eq!(none.deleted_rows().count(), 0);
        let empty_group = DeletionVector::new(binding(1, 1, 1, 0));
        empty_group.write(&path).unwrap();
        let empty_group = DeletionVector::open(&path).unwrap();
        assert_eq!(empty_group.visible_rows().count(), 0);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn out_of_range_marks_are_rejected() {
        let mut vector = DeletionVector::new(binding(1, 1, 1, 10));
        assert!(matches!(
            vector.mark_deleted(10),
            Err(ColumnGroupError::RowOutOfRange { .. })
        ));
        assert!(!vector.is_deleted(10));
    }

    #[test]
    fn corrupt_sidecars_are_rejected() {
        let path = unique_path("corrupt");
        let mut vector = DeletionVector::new(binding(7, 1, 2, 100));
        vector.mark_deleted(42).unwrap();
        vector.write(&path).unwrap();
        let bytes = fs::read(&path).unwrap();
        // Truncations at every region boundary.
        for cut in [0, 4, 12, bytes.len() - 1] {
            fs::write(&path, &bytes[..cut]).unwrap();
            assert!(matches!(
                DeletionVector::open(&path),
                Err(ColumnGroupError::Corrupt(_))
            ));
        }
        // A flipped bitmap byte fails the checksum.
        let mut tampered = bytes.clone();
        tampered[30] ^= 0x10;
        fs::write(&path, &tampered).unwrap();
        assert!(matches!(
            DeletionVector::open(&path),
            Err(ColumnGroupError::Corrupt(_))
        ));
        // A declared count that disagrees with the bitmap fails even with a
        // recomputed checksum.
        let body_start = DELETION_VECTOR_MAGIC.len();
        let footer_start = bytes.len() - FOOTER_BYTES;
        let mut tampered = bytes.clone();
        tampered[body_start + 32..body_start + 36].copy_from_slice(&9u32.to_le_bytes());
        let crc = skein_integrity::crc32c(&tampered[body_start..footer_start]).get();
        tampered[footer_start + 8..footer_start + 12].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &tampered).unwrap();
        let error = DeletionVector::open(&path).unwrap_err();
        assert!(error.to_string().contains("declares"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn publication_fork_preserves_group_ordinals_and_cumulative_deletes() {
        let mut original = DeletionVector::new(binding(7, 2, 4, 100));
        original.mark_deleted(3).unwrap();
        let mut next = original
            .fork_for_publication(ManifestGeneration(5))
            .unwrap();
        next.mark_deleted(90).unwrap();

        assert_eq!(next.group_id(), 7);
        assert_eq!(next.group_generation(), ManifestGeneration(2));
        assert_eq!(next.publication_generation(), ManifestGeneration(5));
        assert_eq!(next.deleted_rows().collect::<Vec<_>>(), vec![3, 90]);
        assert_eq!(original.deleted_rows().collect::<Vec<_>>(), vec![3]);
        assert!(next.fork_for_publication(ManifestGeneration(5)).is_err());
    }

    #[test]
    fn binding_rejects_publication_before_physical_group_creation() {
        assert!(matches!(
            DeletionVectorBinding::new(7, ManifestGeneration(3), ManifestGeneration(2), 100,),
            Err(ColumnGroupError::Unsupported(_))
        ));
    }
}
