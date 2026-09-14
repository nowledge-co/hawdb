//! Internal metadata for the derived graph columnar shadow.
//!
//! Owns stable property-key interning, bounded metadata accounting, and
//! per-generation typed layouts. Graph scanning, publication orchestration,
//! and the admission token remain in the embedded facade.

use crate::durable_replace_file;
use skein_core::{PropertyId, Result, SkeinError, Value};
use skein_integrity::crc32c;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

/// Persistent shadow key dictionary inside the shadow directory.
pub const SHADOW_KEY_DICTIONARY_FILE: &str = "property-keys.skein";
const SHADOW_KEY_DICTIONARY_MAGIC: &[u8; 9] = b"SKNSHKEY1";
const SHADOW_KEY_DICTIONARY_VERSION: u32 = 1;
const MAX_SHADOW_KEY_DICTIONARY_BYTES: u64 = 64 * 1024 * 1024;

/// First shadow key-dictionary id; everything below is reserved.
pub const FIRST_DICTIONARY_COLUMN: u32 = 4;
/// Enforced default budget for pass-1 type state, typed layouts, and the key
/// dictionary. It is part of the up-front admission and every metadata
/// allocation is charged before materialization.
pub const DEFAULT_SHADOW_METADATA_BUDGET_BYTES: u64 = 8 * 1024 * 1024;
/// Conservative container overhead charged for every metadata map/vector
/// entry in addition to owned string bytes.
const SHADOW_KEY_ENTRY_OVERHEAD_BYTES: u64 = 48;
/// Per-table overhead charged for pass-1 state.
pub const SHADOW_PASS1_TABLE_OVERHEAD_BYTES: u64 = 64;
/// Per-property pass-1 entry: one `PropertyId` plus type state in a B-tree.
const SHADOW_PASS1_PROPERTY_OVERHEAD_BYTES: u64 = SHADOW_KEY_ENTRY_OVERHEAD_BYTES;
/// Per-table overhead of the typed layout and its lookup index.
const SHADOW_LAYOUT_TABLE_OVERHEAD_BYTES: u64 = 64;
/// Per typed property: one vector entry plus one B-tree index entry.
const SHADOW_LAYOUT_PROPERTY_OVERHEAD_BYTES: u64 = 2 * SHADOW_KEY_ENTRY_OVERHEAD_BYTES;
// --- shadow key dictionary --------------------------------------------------

/// The shadow's persistent property-key interning: id = `4 + index` in
/// first-seen order, ids never reused or reordered. The file is rewritten
/// whole (temp file, fsync, atomic rename) but its content only ever grows
/// by appending keys, so older generations keep decoding.
#[derive(Debug, Default)]
pub struct ShadowKeyDictionary {
    ids: BTreeMap<String, u32>,
    keys: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct ShadowMetadataBudget {
    limit_bytes: u64,
    used_bytes: u64,
    peak_bytes: u64,
}

impl ShadowMetadataBudget {
    pub fn new(limit_bytes: u64, used_bytes: u64) -> Result<Self> {
        if used_bytes > limit_bytes {
            return Err(Self::exceeded(used_bytes, limit_bytes));
        }
        Ok(Self {
            limit_bytes,
            used_bytes,
            peak_bytes: used_bytes,
        })
    }

    pub fn charge(&mut self, bytes: u64, allocation: &str) -> Result<()> {
        let required = self.used_bytes.checked_add(bytes).ok_or_else(|| {
            SkeinError::Storage(format!(
                "columnar shadow metadata accounting overflows while reserving {allocation}"
            ))
        })?;
        if required > self.limit_bytes {
            return Err(SkeinError::Storage(format!(
                "columnar shadow {allocation} needs {required} metadata bytes, exceeding its \
                 enforced {} byte budget",
                self.limit_bytes
            )));
        }
        self.used_bytes = required;
        self.peak_bytes = self.peak_bytes.max(required);
        Ok(())
    }

    pub fn release(&mut self, bytes: u64) {
        debug_assert!(bytes <= self.used_bytes);
        self.used_bytes = self.used_bytes.saturating_sub(bytes);
    }

    pub const fn used_bytes(self) -> u64 {
        self.used_bytes
    }

    pub const fn peak_bytes(self) -> u64 {
        self.peak_bytes
    }

    fn exceeded(required: u64, limit: u64) -> SkeinError {
        SkeinError::Storage(format!(
            "columnar shadow existing key dictionary needs {required} metadata bytes, exceeding \
             its enforced {limit} byte budget"
        ))
    }
}

impl ShadowKeyDictionary {
    pub fn load(
        shadow_root: &Path,
        metadata_budget_bytes: u64,
    ) -> Result<(Self, ShadowMetadataBudget)> {
        let path = shadow_root.join(SHADOW_KEY_DICTIONARY_FILE);
        let file_bytes = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok((
                    Self::default(),
                    ShadowMetadataBudget::new(metadata_budget_bytes.saturating_mul(2), 0)?,
                ));
            }
            Err(error) => return Err(error.into()),
        };
        if file_bytes > MAX_SHADOW_KEY_DICTIONARY_BYTES {
            return Err(SkeinError::Storage(
                "columnar shadow key dictionary exceeds its size limit".to_string(),
            ));
        }
        // The existing dictionary is baseline state: it is covered by the
        // measured admission, so it charges a scratch account first and
        // then becomes the budget's pre-used baseline. The configured
        // budget bounds only this build's GROWTH — a growth-exhausted
        // attempt fails with its keys durable, and the next attempt's
        // larger baseline admits them: monotone progress, convergence. An
        // existing dictionary can therefore never fail the load short of
        // the absolute size cap above.
        let mut scratch = ShadowMetadataBudget::new(u64::MAX, 0)?;
        scratch.charge(file_bytes, "key dictionary read buffer")?;
        let bytes = fs::read(&path)?;
        let dictionary = Self::decode(&bytes, &mut scratch)?;
        scratch.release(file_bytes);
        let existing_bytes = scratch.used_bytes();
        // Recurring schema-proportional charges (pass-1 table state and
        // layout entries) repeat every attempt and scale with the typed
        // property set, so they must be covered by measurement like the
        // baseline — squeezing them into the fixed growth budget would
        // livelock once the schema outgrows it. Each typed property's
        // pass-1 + layout charge is at most twice its dictionary entry
        // charge, so `2 × existing` bounds the recurring cost of every
        // already-known key and `2 × configured` bounds growth plus the
        // recurring cost of this attempt's new keys:
        // limit = 2·configured + 3·existing, baseline = existing.
        let metadata_budget = ShadowMetadataBudget::new(
            metadata_budget_bytes
                .saturating_mul(2)
                .saturating_add(existing_bytes.saturating_mul(3)),
            existing_bytes,
        )?;
        Ok((dictionary, metadata_budget))
    }

    fn decode(bytes: &[u8], metadata_budget: &mut ShadowMetadataBudget) -> Result<Self> {
        let corrupt = |message: &str| {
            SkeinError::Storage(format!("columnar shadow key dictionary {message}"))
        };
        if bytes.len() as u64 > MAX_SHADOW_KEY_DICTIONARY_BYTES {
            return Err(corrupt("exceeds its size limit"));
        }
        let magic = SHADOW_KEY_DICTIONARY_MAGIC.len();
        let footer = 8 + 4 + magic;
        if bytes.len() < magic + footer
            || &bytes[..magic] != SHADOW_KEY_DICTIONARY_MAGIC
            || &bytes[bytes.len() - magic..] != SHADOW_KEY_DICTIONARY_MAGIC
        {
            return Err(corrupt("framing is invalid"));
        }
        let body = &bytes[magic..bytes.len() - footer];
        let footer_bytes = &bytes[bytes.len() - footer..bytes.len() - magic];
        let stored_len = u64::from_le_bytes(footer_bytes[..8].try_into().expect("8 bytes"));
        let stored_crc = u32::from_le_bytes(footer_bytes[8..12].try_into().expect("4 bytes"));
        if stored_len != body.len() as u64 || crc32c(body).get() != stored_crc {
            return Err(corrupt("checksum or length is invalid"));
        }
        let mut position = 0usize;
        let read_u32 = |position: &mut usize| -> Result<u32> {
            let end = position
                .checked_add(4)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| corrupt("is truncated"))?;
            let value = u32::from_le_bytes(body[*position..end].try_into().expect("4 bytes"));
            *position = end;
            Ok(value)
        };
        if read_u32(&mut position)? != SHADOW_KEY_DICTIONARY_VERSION {
            return Err(corrupt("has an unsupported version"));
        }
        let count = read_u32(&mut position)? as usize;
        let mut dictionary = Self::default();
        for _ in 0..count {
            let length = read_u32(&mut position)? as usize;
            let end = position
                .checked_add(length)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| corrupt("is truncated"))?;
            let key = std::str::from_utf8(&body[position..end])
                .map_err(|_| corrupt("holds a non-UTF-8 key"))?;
            position = end;
            if dictionary.ids.contains_key(key) {
                return Err(corrupt("repeats a key"));
            }
            dictionary.intern(key, metadata_budget)?;
        }
        if position != body.len() {
            return Err(corrupt("has trailing bytes"));
        }
        Ok(dictionary)
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn id(&self, key: &str) -> Option<PropertyId> {
        self.ids.get(key).copied().map(PropertyId)
    }

    /// Estimated resident footprint for a builder's metadata baseline.
    pub fn estimated_bytes(&self) -> u64 {
        self.keys.iter().fold(0u64, |bytes, key| {
            bytes.saturating_add(2 * (key.len() as u64 + SHADOW_KEY_ENTRY_OVERHEAD_BYTES))
        })
    }

    pub fn intern(
        &mut self,
        key: &str,
        metadata_budget: &mut ShadowMetadataBudget,
    ) -> Result<PropertyId> {
        if let Some(id) = self.ids.get(key) {
            return Ok(PropertyId(*id));
        }
        metadata_budget.charge(Self::entry_bytes(key), "key dictionary")?;
        let id = u32::try_from(self.keys.len())
            .ok()
            .and_then(|index| index.checked_add(FIRST_DICTIONARY_COLUMN))
            .ok_or_else(|| {
                SkeinError::Storage(
                    "columnar shadow key dictionary exceeds the u32 id space".to_string(),
                )
            })?;
        self.ids.insert(key.to_string(), id);
        self.keys.push(key.to_string());
        Ok(PropertyId(id))
    }

    fn entry_bytes(key: &str) -> u64 {
        2 * (key.len() as u64 + SHADOW_KEY_ENTRY_OVERHEAD_BYTES)
    }

    pub fn key(&self, id: PropertyId) -> Option<&str> {
        id.0.checked_sub(FIRST_DICTIONARY_COLUMN)
            .and_then(|index| self.keys.get(index as usize))
            .map(String::as_str)
    }

    fn encoded_len(&self) -> Result<u64> {
        let body_len = self.keys.iter().try_fold(8u64, |bytes, key| {
            bytes
                .checked_add(4)
                .and_then(|bytes| bytes.checked_add(key.len() as u64))
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "columnar shadow key dictionary length overflows u64".to_string(),
                    )
                })
        })?;
        body_len
            .checked_add((2 * SHADOW_KEY_DICTIONARY_MAGIC.len() + 12) as u64)
            .ok_or_else(|| {
                SkeinError::Storage(
                    "columnar shadow key dictionary framing length overflows u64".to_string(),
                )
            })
    }

    fn encode(&self, encoded_len: u64) -> Result<Vec<u8>> {
        if encoded_len > MAX_SHADOW_KEY_DICTIONARY_BYTES {
            return Err(SkeinError::Storage(
                "columnar shadow key dictionary exceeds its size limit".to_string(),
            ));
        }
        let capacity = usize::try_from(encoded_len).map_err(|_| {
            SkeinError::Storage(
                "columnar shadow key dictionary exceeds the addressable memory range".to_string(),
            )
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend(SHADOW_KEY_DICTIONARY_MAGIC);
        let body_start = bytes.len();
        bytes.extend(SHADOW_KEY_DICTIONARY_VERSION.to_le_bytes());
        bytes.extend((self.keys.len() as u32).to_le_bytes());
        for key in &self.keys {
            bytes.extend((key.len() as u32).to_le_bytes());
            bytes.extend(key.as_bytes());
        }
        let body_end = bytes.len();
        let body_len = (body_end - body_start) as u64;
        let body_crc = crc32c(&bytes[body_start..body_end]).get();
        bytes.extend(body_len.to_le_bytes());
        bytes.extend(body_crc.to_le_bytes());
        bytes.extend(SHADOW_KEY_DICTIONARY_MAGIC);
        debug_assert_eq!(bytes.len(), capacity);
        Ok(bytes)
    }

    /// Publishes the dictionary durably: temp file, fsync, atomic rename
    /// (never truncating or syncing an already-published handle). Returns
    /// the bytes written.
    pub fn persist(
        &self,
        shadow_root: &Path,
        metadata_budget: &mut ShadowMetadataBudget,
    ) -> Result<u64> {
        let encoded_len = self.encoded_len()?;
        metadata_budget.charge(encoded_len, "key dictionary serialization")?;
        let bytes = match self.encode(encoded_len) {
            Ok(bytes) => bytes,
            Err(error) => {
                metadata_budget.release(encoded_len);
                return Err(error);
            }
        };
        let path = shadow_root.join(SHADOW_KEY_DICTIONARY_FILE);
        let tmp_path = shadow_root.join(format!(".{SHADOW_KEY_DICTIONARY_FILE}.tmp"));
        let result = (|| -> Result<()> {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            durable_replace_file(&tmp_path, &path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        metadata_budget.release(encoded_len);
        result.map(|()| bytes.len() as u64)
    }
}

// --- per-table build --------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredType {
    Int,
    Float,
    Bool,
    Str,
    Binary,
    Residual,
}

impl InferredType {
    fn of(value: &Value) -> Self {
        match value {
            Value::Int(_) => Self::Int,
            Value::Float(_) => Self::Float,
            Value::Bool(_) => Self::Bool,
            Value::String(_) => Self::Str,
            Value::Binary(_) => Self::Binary,
            Value::Null | Value::Uuid(_) | Value::List(_) | Value::Map(_) => Self::Residual,
        }
    }

    fn merge(self, other: Self) -> Self {
        if self == other {
            self
        } else {
            Self::Residual
        }
    }
}

/// Per-table pass-1 accumulator: O(1) state per property id (current
/// type-lattice point), never buffered rows and never another owned key.
#[derive(Debug, Default)]
pub struct TablePropertyTypes {
    inferred: BTreeMap<PropertyId, InferredType>,
}

impl TablePropertyTypes {
    pub fn observe(
        &mut self,
        properties: &BTreeMap<String, Value>,
        dictionary: &mut ShadowKeyDictionary,
        metadata_budget: &mut ShadowMetadataBudget,
    ) -> Result<()> {
        for (key, value) in properties {
            let observed = InferredType::of(value);
            let property_id = dictionary.intern(key, metadata_budget)?;
            if !self.inferred.contains_key(&property_id) {
                metadata_budget.charge(
                    SHADOW_PASS1_PROPERTY_OVERHEAD_BYTES,
                    "pass-1 property state",
                )?;
            }
            self.inferred
                .entry(property_id)
                .and_modify(|current| *current = current.merge(observed))
                .or_insert(observed);
        }
        Ok(())
    }
}

/// The fixed column layout of one dirty table for this generation, derived
/// from pass 1 before any row is buffered.
pub struct ShadowTableLayout {
    /// Typed property ids in stable dictionary-id order.
    typed: Vec<PropertyId>,
    /// Precomputed property id -> typed column position, so wide-table row
    /// appends stay O(P log C) instead of the quadratic per-property scan.
    typed_index: BTreeMap<PropertyId, usize>,
}

impl ShadowTableLayout {
    pub fn typed(&self) -> &[PropertyId] {
        &self.typed
    }

    pub fn typed_index(&self) -> &BTreeMap<PropertyId, usize> {
        &self.typed_index
    }
}

pub fn shadow_table_layout(
    types: &TablePropertyTypes,
    metadata_budget: &mut ShadowMetadataBudget,
) -> Result<ShadowTableLayout> {
    metadata_budget.charge(SHADOW_LAYOUT_TABLE_OVERHEAD_BYTES, "typed table layout")?;
    let mut typed = Vec::new();
    let mut typed_index = BTreeMap::new();
    for (property_id, inferred) in &types.inferred {
        if *inferred != InferredType::Residual {
            metadata_budget.charge(
                SHADOW_LAYOUT_PROPERTY_OVERHEAD_BYTES,
                "typed property layout",
            )?;
            typed_index.insert(*property_id, typed.len());
            typed.push(*property_id);
        }
    }
    Ok(ShadowTableLayout { typed, typed_index })
}

#[cfg(test)]
mod tests;
