//! Node group artifact: id-ordered rows stored column-wise (§3.1, §3.5.2).
//!
//! File layout:
//!
//! ```text
//! SKNCOLG01 | id chunk | column chunks ... | directory
//!           | u64 directory length | u32 crc32c(directory) | SKNCOLG01
//! ```
//!
//! The directory is a fixed-layout offset table in the FlatBuffers style:
//! scalars at fixed offsets, the column-entry array addressed by an
//! (offset, stride, count) triple, and per-entry zone maps as fixed-size
//! records — a reader opens the checksummed footer and addresses any chunk
//! directly without materializing a parsed object graph. Columns are keyed
//! by interned `PropertyId` (§3.5.3(c)); key strings never appear in the
//! artifact.

use super::deletion::DeletionVector;
use super::encoding::{
    bits_for, decode_chunk, encode_chunk_auto, pack_values, unpack_values, ChunkEncoding, Cursor,
};
use super::zone::{ChunkZoneMap, ZONE_MAP_RECORD_BYTES};
use super::{corrupt, unsupported, ColumnGroupError, COLUMN_GROUP_MAGIC};
use crate::durability::durable_replace_file;
use crate::ManifestGeneration;
use skein_core::{PropertyId, Value};
use skein_integrity::crc32c;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

/// Default fixed row capacity of one node group (§2: node group).
pub const DEFAULT_GROUP_ROW_CAPACITY: u32 = 65_536;
/// Encoding id of the delta-encoded, bit-packed id column.
pub const ID_CHUNK_ENCODING: u8 = 8;

const DIRECTORY_VERSION: u32 = 1;
const DIRECTORY_HEADER_BYTES: usize = 88;
const COLUMN_ENTRY_STRIDE: usize = 32 + ZONE_MAP_RECORD_BYTES;
const FOOTER_BYTES: usize = 8 + 4 + COLUMN_GROUP_MAGIC.len();
const HEADER_BYTES: u64 = COLUMN_GROUP_MAGIC.len() as u64;

/// Configuration of the group writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnGroupConfig {
    /// Maximum rows in one group.
    pub row_capacity: u32,
    /// Whether chunk bodies may be zstd-compressed.
    pub compress: bool,
}

impl Default for ColumnGroupConfig {
    fn default() -> Self {
        Self {
            row_capacity: DEFAULT_GROUP_ROW_CAPACITY,
            compress: true,
        }
    }
}

/// Extent and integrity facts of the id column chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdChunkDescriptor {
    pub offset: u64,
    pub length: u64,
    pub crc32c: u32,
}

/// Directory entry of one column chunk, addressable independently of its
/// siblings (§3.1.2).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkDescriptor {
    pub property_id: PropertyId,
    pub encoding: ChunkEncoding,
    pub compressed: bool,
    pub offset: u64,
    pub length: u64,
    pub crc32c: u32,
    pub zone_map: ChunkZoneMap,
}

/// The decoded group directory: every fact needed to plan and execute reads
/// without touching chunk bytes (§3.5.3(b)).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnGroupDirectory {
    pub group_id: u64,
    pub generation: ManifestGeneration,
    pub row_capacity: u32,
    pub row_count: u32,
    pub min_id: u64,
    pub max_id: u64,
    pub id_chunk: IdChunkDescriptor,
    /// Sorted by ascending property id.
    pub columns: Vec<ColumnChunkDescriptor>,
}

impl ColumnGroupDirectory {
    /// The directory entry for a property, if the group stores it.
    pub fn column(&self, property_id: PropertyId) -> Option<&ColumnChunkDescriptor> {
        self.columns
            .binary_search_by_key(&property_id.0, |column| column.property_id.0)
            .ok()
            .map(|index| &self.columns[index])
    }

    fn encode(&self) -> Result<Vec<u8>, ColumnGroupError> {
        let mut bytes =
            Vec::with_capacity(DIRECTORY_HEADER_BYTES + self.columns.len() * COLUMN_ENTRY_STRIDE);
        bytes.extend(DIRECTORY_VERSION.to_le_bytes());
        bytes.extend(self.row_capacity.to_le_bytes());
        bytes.extend(self.group_id.to_le_bytes());
        bytes.extend(self.generation.0.to_le_bytes());
        bytes.extend(self.row_count.to_le_bytes());
        let column_count = u32::try_from(self.columns.len())
            .map_err(|_| unsupported("group column count exceeds u32".to_string()))?;
        bytes.extend(column_count.to_le_bytes());
        bytes.extend(self.min_id.to_le_bytes());
        bytes.extend(self.max_id.to_le_bytes());
        bytes.extend(self.id_chunk.offset.to_le_bytes());
        bytes.extend(self.id_chunk.length.to_le_bytes());
        bytes.extend(self.id_chunk.crc32c.to_le_bytes());
        bytes.push(ID_CHUNK_ENCODING);
        bytes.push(0);
        bytes.extend(0u16.to_le_bytes());
        bytes.extend((DIRECTORY_HEADER_BYTES as u64).to_le_bytes());
        bytes.extend((COLUMN_ENTRY_STRIDE as u32).to_le_bytes());
        bytes.extend(0u32.to_le_bytes());
        debug_assert_eq!(bytes.len(), DIRECTORY_HEADER_BYTES);
        for column in &self.columns {
            bytes.extend(column.property_id.0.to_le_bytes());
            bytes.push(column.encoding.id());
            bytes.push(u8::from(column.compressed));
            bytes.extend(0u16.to_le_bytes());
            bytes.extend(column.offset.to_le_bytes());
            bytes.extend(column.length.to_le_bytes());
            bytes.extend(column.crc32c.to_le_bytes());
            bytes.extend(0u32.to_le_bytes());
            bytes.extend(column.zone_map.encode()?);
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8], data_end: u64) -> Result<Self, ColumnGroupError> {
        let mut cursor = Cursor::new(bytes);
        let version = cursor.read_u32("group directory version")?;
        if version != DIRECTORY_VERSION {
            return Err(corrupt(format!(
                "unsupported group directory version {version}"
            )));
        }
        let row_capacity = cursor.read_u32("group row capacity")?;
        let group_id = cursor.read_u64("group id")?;
        let generation = ManifestGeneration(cursor.read_u64("group generation")?);
        let row_count = cursor.read_u32("group row count")?;
        let column_count = cursor.read_u32("group column count")?;
        let min_id = cursor.read_u64("group min record id")?;
        let max_id = cursor.read_u64("group max record id")?;
        if row_capacity == 0 || row_count > row_capacity {
            return Err(corrupt(format!(
                "group row count {row_count} exceeds its capacity {row_capacity}"
            )));
        }
        if row_count > 0 && min_id > max_id {
            return Err(corrupt("group record id bounds are inverted".to_string()));
        }
        let id_chunk = IdChunkDescriptor {
            offset: cursor.read_u64("id chunk offset")?,
            length: cursor.read_u64("id chunk length")?,
            crc32c: cursor.read_u32("id chunk checksum")?,
        };
        validate_extent(id_chunk.offset, id_chunk.length, data_end, "id chunk")?;
        let id_encoding = cursor.read_u8("id chunk encoding")?;
        if id_encoding != ID_CHUNK_ENCODING {
            return Err(corrupt(format!("unknown id chunk encoding {id_encoding}")));
        }
        let id_flags = cursor.read_u8("id chunk flags")?;
        if id_flags != 0 {
            return Err(corrupt("id chunk flags are invalid".to_string()));
        }
        cursor.read_bytes(2, "id chunk reserved bytes")?;
        let entries_offset = cursor.read_u64("column entry array offset")?;
        let entry_stride = cursor.read_u32("column entry stride")?;
        cursor.read_bytes(4, "directory reserved bytes")?;
        if entries_offset != DIRECTORY_HEADER_BYTES as u64
            || entry_stride != COLUMN_ENTRY_STRIDE as u32
        {
            return Err(corrupt(
                "group directory column array layout is invalid".to_string(),
            ));
        }
        let expected_len = DIRECTORY_HEADER_BYTES
            .checked_add(
                (column_count as usize)
                    .checked_mul(COLUMN_ENTRY_STRIDE)
                    .ok_or_else(|| corrupt("group column count overflows".to_string()))?,
            )
            .ok_or_else(|| corrupt("group column count overflows".to_string()))?;
        if bytes.len() != expected_len {
            return Err(corrupt(format!(
                "group directory holds {} bytes but {column_count} columns need {expected_len}",
                bytes.len()
            )));
        }
        let mut columns = Vec::with_capacity(column_count as usize);
        let mut previous_property: Option<u32> = None;
        for _ in 0..column_count {
            let property_id = cursor.read_u32("column property id")?;
            if previous_property.is_some_and(|previous| previous >= property_id) {
                return Err(corrupt(
                    "group directory columns are not sorted by property id".to_string(),
                ));
            }
            previous_property = Some(property_id);
            let encoding = ChunkEncoding::from_id(cursor.read_u8("column encoding id")?)?;
            let flags = cursor.read_u8("column flags")?;
            if flags > 1 {
                return Err(corrupt(format!("column flags {flags} are invalid")));
            }
            cursor.read_bytes(2, "column reserved bytes")?;
            let offset = cursor.read_u64("column chunk offset")?;
            let length = cursor.read_u64("column chunk length")?;
            let checksum = cursor.read_u32("column chunk checksum")?;
            cursor.read_bytes(4, "column reserved bytes")?;
            validate_extent(offset, length, data_end, "column chunk")?;
            let zone_map = ChunkZoneMap::decode(&mut cursor)?;
            if zone_map.row_count() != u64::from(row_count) {
                return Err(corrupt(format!(
                    "column zone map covers {} rows in a {row_count} row group",
                    zone_map.row_count()
                )));
            }
            columns.push(ColumnChunkDescriptor {
                property_id: PropertyId(property_id),
                encoding,
                compressed: flags == 1,
                offset,
                length,
                crc32c: checksum,
                zone_map,
            });
        }
        cursor.expect_exhausted("group directory")?;
        Ok(Self {
            group_id,
            generation,
            row_capacity,
            row_count,
            min_id,
            max_id,
            id_chunk,
            columns,
        })
    }
}

fn validate_extent(
    offset: u64,
    length: u64,
    data_end: u64,
    what: &str,
) -> Result<(), ColumnGroupError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| corrupt(format!("{what} extent overflows")))?;
    if offset < HEADER_BYTES || end > data_end {
        return Err(corrupt(format!(
            "{what} extent [{offset}, {end}) is outside the data region \
             [{HEADER_BYTES}, {data_end})"
        )));
    }
    Ok(())
}

// --- id column codec --------------------------------------------------------

fn encode_ids(ids: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend((ids.len() as u32).to_le_bytes());
    let Some(first) = ids.first() else {
        return bytes;
    };
    let deltas = ids
        .windows(2)
        .map(|window| window[1] - window[0])
        .collect::<Vec<_>>();
    let width = bits_for(deltas.iter().copied().max().unwrap_or(0));
    bytes.extend(first.to_le_bytes());
    bytes.push(width);
    bytes.extend(pack_values(&deltas, width));
    bytes
}

fn decode_ids(bytes: &[u8], expected_rows: u32) -> Result<Vec<u64>, ColumnGroupError> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.read_u32("id chunk row count")?;
    if count != expected_rows {
        return Err(corrupt(format!(
            "id chunk holds {count} rows but the directory declares {expected_rows}"
        )));
    }
    if count == 0 {
        cursor.expect_exhausted("id chunk")?;
        return Ok(Vec::new());
    }
    let first = cursor.read_u64("first record id")?;
    let width = cursor.read_u8("id delta width")?;
    let deltas = unpack_values(&mut cursor, width, count as usize - 1, "id deltas")?;
    let mut ids = Vec::with_capacity(count as usize);
    ids.push(first);
    let mut current = first;
    for delta in deltas {
        if delta == 0 {
            return Err(corrupt(
                "record ids are not strictly increasing".to_string(),
            ));
        }
        current = current
            .checked_add(delta)
            .ok_or_else(|| corrupt("record id delta overflows the u64 id space".to_string()))?;
        ids.push(current);
    }
    cursor.expect_exhausted("id chunk")?;
    Ok(ids)
}

// --- writer -----------------------------------------------------------------

/// Writes node groups with the temp-file, fsync, rename publish protocol.
#[derive(Debug, Clone, Copy, Default)]
pub struct ColumnGroupWriter {
    config: ColumnGroupConfig,
}

impl ColumnGroupWriter {
    pub fn new(config: ColumnGroupConfig) -> Self {
        Self { config }
    }

    /// Writes one group: `ids` are the strictly increasing record ids and
    /// each column supplies one `Value` per row (`Value::Null` = absent).
    pub fn write(
        &self,
        path: &Path,
        group_id: u64,
        generation: ManifestGeneration,
        ids: &[u64],
        columns: &[(PropertyId, Vec<Value>)],
    ) -> Result<ColumnGroupDirectory, ColumnGroupError> {
        self.validate(ids, columns)?;
        let tmp_path = path.with_extension("skein.tmp");
        let result = self.write_inner(&tmp_path, group_id, generation, ids, columns);
        let directory = match result {
            Ok(directory) => directory,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        durable_replace_file(&tmp_path, path)?;
        Ok(directory)
    }

    fn validate(
        &self,
        ids: &[u64],
        columns: &[(PropertyId, Vec<Value>)],
    ) -> Result<(), ColumnGroupError> {
        if ids.len() > self.config.row_capacity as usize {
            return Err(unsupported(format!(
                "group holds {} rows, exceeding its {} row capacity",
                ids.len(),
                self.config.row_capacity
            )));
        }
        if ids.windows(2).any(|window| window[0] >= window[1]) {
            return Err(unsupported(
                "group record ids must be strictly increasing".to_string(),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (property_id, values) in columns {
            if !seen.insert(property_id.0) {
                return Err(unsupported(format!(
                    "group declares property {} twice",
                    property_id.0
                )));
            }
            if values.len() != ids.len() {
                return Err(unsupported(format!(
                    "column {} holds {} values for {} rows",
                    property_id.0,
                    values.len(),
                    ids.len()
                )));
            }
        }
        Ok(())
    }

    fn write_inner(
        &self,
        path: &Path,
        group_id: u64,
        generation: ManifestGeneration,
        ids: &[u64],
        columns: &[(PropertyId, Vec<Value>)],
    ) -> Result<ColumnGroupDirectory, ColumnGroupError> {
        let mut file = File::create(path)?;
        let mut offset = 0u64;
        file.write_all(COLUMN_GROUP_MAGIC)?;
        offset += HEADER_BYTES;

        let id_bytes = encode_ids(ids);
        let id_chunk = IdChunkDescriptor {
            offset,
            length: id_bytes.len() as u64,
            crc32c: crc32c(&id_bytes).get(),
        };
        file.write_all(&id_bytes)?;
        offset += id_bytes.len() as u64;

        let mut sorted = columns.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|(property_id, _)| property_id.0);
        let mut descriptors = Vec::with_capacity(sorted.len());
        for (property_id, values) in sorted {
            let chunk = encode_chunk_auto(values, self.config.compress)?;
            let descriptor = ColumnChunkDescriptor {
                property_id: *property_id,
                encoding: chunk.encoding,
                compressed: chunk.compressed,
                offset,
                length: chunk.bytes.len() as u64,
                crc32c: crc32c(&chunk.bytes).get(),
                zone_map: ChunkZoneMap::build(values),
            };
            file.write_all(&chunk.bytes)?;
            offset += chunk.bytes.len() as u64;
            descriptors.push(descriptor);
        }

        let directory = ColumnGroupDirectory {
            group_id,
            generation,
            row_capacity: self.config.row_capacity,
            row_count: ids.len() as u32,
            min_id: ids.first().copied().unwrap_or(0),
            max_id: ids.last().copied().unwrap_or(0),
            id_chunk,
            columns: descriptors,
        };
        let directory_bytes = directory.encode()?;
        file.write_all(&directory_bytes)?;
        file.write_all(&(directory_bytes.len() as u64).to_le_bytes())?;
        file.write_all(&crc32c(&directory_bytes).get().to_le_bytes())?;
        file.write_all(COLUMN_GROUP_MAGIC)?;
        file.sync_all()?;
        Ok(directory)
    }
}

// --- byte source ------------------------------------------------------------

/// Byte-range access to a group artifact. The indirection exists so tests
/// can observe exactly which extents a read touches (§3.2.3).
pub trait ColumnGroupByteSource {
    fn byte_len(&self) -> Result<u64, ColumnGroupError>;
    fn read_at(&self, offset: u64, length: u64) -> Result<Vec<u8>, ColumnGroupError>;
}

/// File-backed byte source using positioned reads.
#[derive(Debug)]
pub struct FileByteSource {
    file: File,
}

impl FileByteSource {
    pub fn open(path: &Path) -> Result<Self, ColumnGroupError> {
        Ok(Self {
            file: File::open(path)?,
        })
    }
}

impl ColumnGroupByteSource for FileByteSource {
    fn byte_len(&self) -> Result<u64, ColumnGroupError> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, offset: u64, length: u64) -> Result<Vec<u8>, ColumnGroupError> {
        let length = usize::try_from(length)
            .map_err(|_| corrupt(format!("read of {length} bytes is not addressable")))?;
        let mut buffer = vec![0u8; length];
        read_exact_at(&self.file, &mut buffer, offset)?;
        Ok(buffer)
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buffer, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buffer.is_empty() {
        let read = file.seek_read(buffer, offset)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "reached end of file before filling the buffer",
            ));
        }
        buffer = &mut buffer[read..];
        offset += read as u64;
    }
    Ok(())
}

// --- reader -----------------------------------------------------------------

/// Opens a group by validating the checksummed footer, then serves chunk
/// reads that touch only the requested extents.
#[derive(Debug)]
pub struct ColumnGroupReader<S: ColumnGroupByteSource> {
    source: S,
    directory: ColumnGroupDirectory,
}

impl ColumnGroupReader<FileByteSource> {
    pub fn open_path(path: &Path) -> Result<Self, ColumnGroupError> {
        Self::open(FileByteSource::open(path)?)
    }
}

impl<S: ColumnGroupByteSource> ColumnGroupReader<S> {
    pub fn open(source: S) -> Result<Self, ColumnGroupError> {
        let len = source.byte_len()?;
        let minimum = HEADER_BYTES + FOOTER_BYTES as u64;
        if len < minimum {
            return Err(corrupt(format!(
                "group artifact holds {len} bytes, below the {minimum} byte minimum"
            )));
        }
        let header = source.read_at(0, HEADER_BYTES)?;
        if header != COLUMN_GROUP_MAGIC {
            return Err(corrupt(
                "group artifact header magic is invalid".to_string(),
            ));
        }
        let footer = source.read_at(len - FOOTER_BYTES as u64, FOOTER_BYTES as u64)?;
        if &footer[12..] != COLUMN_GROUP_MAGIC {
            return Err(corrupt(
                "group artifact footer magic is invalid".to_string(),
            ));
        }
        let directory_len = u64::from_le_bytes(footer[..8].try_into().expect("8B"));
        let stored_crc = u32::from_le_bytes(footer[8..12].try_into().expect("4B"));
        let directory_end = len - FOOTER_BYTES as u64;
        let directory_start = directory_end
            .checked_sub(directory_len)
            .filter(|start| *start >= HEADER_BYTES)
            .ok_or_else(|| {
                corrupt(format!(
                    "group directory length {directory_len} exceeds the artifact"
                ))
            })?;
        let directory_bytes = source.read_at(directory_start, directory_len)?;
        if crc32c(&directory_bytes).get() != stored_crc {
            return Err(corrupt(
                "group directory checksum does not match its contents".to_string(),
            ));
        }
        let directory = ColumnGroupDirectory::decode(&directory_bytes, directory_start)?;
        Ok(Self { source, directory })
    }

    pub fn directory(&self) -> &ColumnGroupDirectory {
        &self.directory
    }

    pub fn row_count(&self) -> u32 {
        self.directory.row_count
    }

    /// Decodes the id column.
    pub fn read_ids(&self) -> Result<Vec<u64>, ColumnGroupError> {
        let chunk = self.directory.id_chunk;
        let bytes = self.source.read_at(chunk.offset, chunk.length)?;
        if crc32c(&bytes).get() != chunk.crc32c {
            return Err(corrupt(
                "id chunk checksum does not match its contents".to_string(),
            ));
        }
        decode_ids(&bytes, self.directory.row_count)
    }

    fn read_column_chunk(
        &self,
        column: &ColumnChunkDescriptor,
    ) -> Result<Vec<Value>, ColumnGroupError> {
        let bytes = self.source.read_at(column.offset, column.length)?;
        if crc32c(&bytes).get() != column.crc32c {
            return Err(corrupt(format!(
                "column chunk {} checksum does not match its contents",
                column.property_id.0
            )));
        }
        let values = decode_chunk(&bytes, column.encoding, column.compressed)?;
        if values.len() != self.directory.row_count as usize {
            return Err(corrupt(format!(
                "column chunk {} decodes {} rows in a {} row group",
                column.property_id.0,
                values.len(),
                self.directory.row_count
            )));
        }
        Ok(values)
    }

    /// Decodes one column, honoring the validity bitmap: the result holds
    /// one `Value` per selected row, `Value::Null` at null positions. With
    /// `selection`, only those row indices are returned, in order.
    pub fn read_column(
        &self,
        property_id: PropertyId,
        selection: Option<&[u32]>,
    ) -> Result<Vec<Value>, ColumnGroupError> {
        let column = self
            .directory
            .column(property_id)
            .ok_or(ColumnGroupError::PropertyMissing(property_id))?;
        let values = self.read_column_chunk(column)?;
        let Some(selection) = selection else {
            return Ok(values);
        };
        selection
            .iter()
            .map(|row| {
                usize::try_from(*row)
                    .ok()
                    .filter(|row| *row < values.len())
                    .map(|row| values[row].clone())
                    .ok_or(ColumnGroupError::RowOutOfRange {
                        row_index: *row,
                        row_count: self.directory.row_count,
                    })
            })
            .collect()
    }

    /// The rows of this group still visible under a deletion vector,
    /// ascending. The vector must be bound to this group and its publishing
    /// generation (§3.5.3(d)); any other binding is rejected.
    pub fn visible_rows<'a>(
        &self,
        deletion_vector: &'a DeletionVector,
    ) -> Result<impl Iterator<Item = u32> + 'a, ColumnGroupError> {
        if deletion_vector.group_id() != self.directory.group_id
            || deletion_vector.generation() != self.directory.generation
        {
            return Err(ColumnGroupError::DeletionVectorMismatch {
                expected_group: self.directory.group_id,
                actual_group: deletion_vector.group_id(),
                expected_generation: self.directory.generation.0,
                actual_generation: deletion_vector.generation().0,
            });
        }
        if deletion_vector.row_count() != self.directory.row_count {
            return Err(corrupt(format!(
                "deletion vector covers {} rows of a {} row group",
                deletion_vector.row_count(),
                self.directory.row_count
            )));
        }
        Ok(deletion_vector.visible_rows())
    }

    /// Point read: reconstructs the requested properties of one row,
    /// touching only the chunks of requested columns (§3.2.3). A property
    /// the group does not store reads as `Value::Null`.
    pub fn read_row(
        &self,
        row_index: u32,
        properties: &[PropertyId],
    ) -> Result<Vec<Value>, ColumnGroupError> {
        if row_index >= self.directory.row_count {
            return Err(ColumnGroupError::RowOutOfRange {
                row_index,
                row_count: self.directory.row_count,
            });
        }
        properties
            .iter()
            .map(|property_id| match self.directory.column(*property_id) {
                Some(column) => {
                    let mut values = self.read_column_chunk(column)?;
                    Ok(std::mem::replace(
                        &mut values[row_index as usize],
                        Value::Null,
                    ))
                }
                None => Ok(Value::Null),
            })
            .collect()
    }
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
        std::env::temp_dir().join(format!("skein-column-group-{name}-{nonce}.skein"))
    }

    fn sample_columns(rows: usize) -> Vec<(PropertyId, Vec<Value>)> {
        vec![
            (
                PropertyId(3),
                (0..rows)
                    .map(|row| {
                        if row % 5 == 0 {
                            Value::Null
                        } else {
                            Value::Int(row as i64 * 7 - 100)
                        }
                    })
                    .collect(),
            ),
            (
                PropertyId(1),
                (0..rows)
                    .map(|row| Value::String(format!("name-{}", row % 17)))
                    .collect(),
            ),
            (
                PropertyId(9),
                (0..rows).map(|row| Value::Bool(row % 3 == 0)).collect(),
            ),
        ]
    }

    #[test]
    fn group_round_trips_ids_and_columns() {
        let path = unique_path("round_trip");
        let ids = (0..500u64).map(|index| index * 3 + 11).collect::<Vec<_>>();
        let columns = sample_columns(ids.len());
        let written = ColumnGroupWriter::default()
            .write(&path, 42, ManifestGeneration(7), &ids, &columns)
            .unwrap();
        assert_eq!(written.min_id, 11);
        assert_eq!(written.max_id, 11 + 499 * 3);
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        assert_eq!(reader.directory(), &written);
        assert_eq!(reader.read_ids().unwrap(), ids);
        // The directory lists columns sorted by property id.
        assert_eq!(
            reader
                .directory()
                .columns
                .iter()
                .map(|column| column.property_id.0)
                .collect::<Vec<_>>(),
            vec![1, 3, 9]
        );
        for (property_id, values) in &columns {
            assert_eq!(
                reader.read_column(*property_id, None).unwrap(),
                *values,
                "property {}",
                property_id.0
            );
        }
        assert!(matches!(
            reader.read_column(PropertyId(999), None),
            Err(ColumnGroupError::PropertyMissing(PropertyId(999)))
        ));
        // No temp file remains after publish.
        assert!(!path.with_extension("skein.tmp").exists());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn empty_group_round_trips() {
        let path = unique_path("empty");
        let written = ColumnGroupWriter::default()
            .write(&path, 1, ManifestGeneration(1), &[], &[])
            .unwrap();
        assert_eq!(written.row_count, 0);
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        assert_eq!(reader.read_ids().unwrap(), Vec::<u64>::new());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn id_column_survives_sparse_ids_at_the_u64_boundary() {
        let path = unique_path("id_boundary");
        let ids = vec![
            3,
            u64::MAX / 2,
            u64::MAX - 65_536,
            u64::MAX - 2,
            u64::MAX - 1,
            u64::MAX,
        ];
        let columns = vec![(
            PropertyId(0),
            ids.iter().map(|id| Value::Int(*id as i64)).collect(),
        )];
        ColumnGroupWriter::default()
            .write(&path, 2, ManifestGeneration(1), &ids, &columns)
            .unwrap();
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        assert_eq!(reader.read_ids().unwrap(), ids);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn writer_rejects_invalid_input() {
        let path = unique_path("invalid_input");
        let writer = ColumnGroupWriter::default();
        // Unsorted ids.
        assert!(matches!(
            writer.write(&path, 1, ManifestGeneration(1), &[5, 5], &[]),
            Err(ColumnGroupError::Unsupported(_))
        ));
        // Column length mismatch.
        assert!(matches!(
            writer.write(
                &path,
                1,
                ManifestGeneration(1),
                &[1, 2],
                &[(PropertyId(0), vec![Value::Int(1)])]
            ),
            Err(ColumnGroupError::Unsupported(_))
        ));
        // Duplicate property.
        assert!(matches!(
            writer.write(
                &path,
                1,
                ManifestGeneration(1),
                &[1],
                &[
                    (PropertyId(0), vec![Value::Int(1)]),
                    (PropertyId(0), vec![Value::Int(2)])
                ]
            ),
            Err(ColumnGroupError::Unsupported(_))
        ));
        // Capacity overflow.
        let small = ColumnGroupWriter::new(ColumnGroupConfig {
            row_capacity: 2,
            compress: false,
        });
        assert!(matches!(
            small.write(&path, 1, ManifestGeneration(1), &[1, 2, 3], &[]),
            Err(ColumnGroupError::Unsupported(_))
        ));
        assert!(!path.exists());
    }

    #[test]
    fn truncated_artifacts_are_corrupt() {
        let path = unique_path("truncated");
        let ids = (0..64u64).collect::<Vec<_>>();
        let columns = sample_columns(ids.len());
        ColumnGroupWriter::default()
            .write(&path, 3, ManifestGeneration(2), &ids, &columns)
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        for cut in [0, 5, 12, bytes.len() / 2, bytes.len() - 1] {
            fs::write(&path, &bytes[..cut]).unwrap();
            assert!(
                matches!(
                    ColumnGroupReader::open_path(&path),
                    Err(ColumnGroupError::Corrupt(_))
                ),
                "cut at {cut}"
            );
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn directory_and_chunk_corruption_is_detected() {
        let path = unique_path("bitflip");
        let ids = (0..128u64).collect::<Vec<_>>();
        let columns = sample_columns(ids.len());
        ColumnGroupWriter::default()
            .write(&path, 4, ManifestGeneration(3), &ids, &columns)
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        let footer_start = bytes.len() - FOOTER_BYTES;
        let directory_len =
            u64::from_le_bytes(bytes[footer_start..footer_start + 8].try_into().unwrap()) as usize;
        let directory_start = footer_start - directory_len;
        // Flip one byte inside the directory: the footer CRC catches it.
        let mut tampered = bytes.clone();
        tampered[directory_start + 20] ^= 0x01;
        fs::write(&path, &tampered).unwrap();
        assert!(matches!(
            ColumnGroupReader::open_path(&path),
            Err(ColumnGroupError::Corrupt(_))
        ));
        // Flip one byte inside a chunk body: the open path succeeds (chunks
        // are not read) and the per-chunk CRC catches it on first read.
        let mut tampered = bytes.clone();
        tampered[HEADER_BYTES as usize + 2] ^= 0x40;
        fs::write(&path, &tampered).unwrap();
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        assert!(matches!(
            reader.read_ids(),
            Err(ColumnGroupError::Corrupt(_))
        ));
        let first_column = reader.directory().columns[0].clone();
        let mut tampered = bytes.clone();
        tampered[first_column.offset as usize] ^= 0x80;
        fs::write(&path, &tampered).unwrap();
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        assert!(matches!(
            reader.read_column(first_column.property_id, None),
            Err(ColumnGroupError::Corrupt(_))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn out_of_range_chunk_extents_are_corrupt() {
        let path = unique_path("extent");
        let ids = (0..32u64).collect::<Vec<_>>();
        let columns = sample_columns(ids.len());
        ColumnGroupWriter::default()
            .write(&path, 5, ManifestGeneration(4), &ids, &columns)
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        let footer_start = bytes.len() - FOOTER_BYTES;
        let directory_len =
            u64::from_le_bytes(bytes[footer_start..footer_start + 8].try_into().unwrap()) as usize;
        let directory_start = footer_start - directory_len;
        // Point the first column entry's offset past the data region and
        // recompute the directory CRC so only the extent check can object.
        let mut tampered = bytes.clone();
        let entry_offset = directory_start + DIRECTORY_HEADER_BYTES + 8;
        tampered[entry_offset..entry_offset + 8]
            .copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        let new_crc = crc32c(&tampered[directory_start..footer_start]).get();
        tampered[footer_start + 8..footer_start + 12].copy_from_slice(&new_crc.to_le_bytes());
        fs::write(&path, &tampered).unwrap();
        let error = ColumnGroupReader::open_path(&path).unwrap_err();
        assert!(matches!(error, ColumnGroupError::Corrupt(_)));
        assert!(error.to_string().contains("extent"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn deletion_vectors_bind_to_group_and_generation() {
        let path = unique_path("dv_binding");
        let ids = (0..40u64).collect::<Vec<_>>();
        let columns = sample_columns(ids.len());
        ColumnGroupWriter::default()
            .write(&path, 7, ManifestGeneration(9), &ids, &columns)
            .unwrap();
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        let mut vector = DeletionVector::new(7, ManifestGeneration(9), 40);
        vector.mark_deleted(0).unwrap();
        vector.mark_deleted(39).unwrap();
        let visible = reader.visible_rows(&vector).unwrap().collect::<Vec<_>>();
        assert_eq!(visible.len(), 38);
        assert_eq!(visible.first(), Some(&1));
        assert_eq!(visible.last(), Some(&38));
        // Wrong generation.
        let stale = DeletionVector::new(7, ManifestGeneration(8), 40);
        assert!(matches!(
            reader.visible_rows(&stale).map(|_| ()),
            Err(ColumnGroupError::DeletionVectorMismatch { .. })
        ));
        // Wrong group.
        let foreign = DeletionVector::new(6, ManifestGeneration(9), 40);
        assert!(matches!(
            reader.visible_rows(&foreign).map(|_| ()),
            Err(ColumnGroupError::DeletionVectorMismatch { .. })
        ));
        // Wrong row count.
        let short = DeletionVector::new(7, ManifestGeneration(9), 39);
        assert!(matches!(
            reader.visible_rows(&short).map(|_| ()),
            Err(ColumnGroupError::Corrupt(_))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn point_reads_reconstruct_rows_and_selections_project() {
        let path = unique_path("point_read");
        let ids = (0..200u64).map(|index| index * 2).collect::<Vec<_>>();
        let columns = sample_columns(ids.len());
        ColumnGroupWriter::default()
            .write(&path, 6, ManifestGeneration(5), &ids, &columns)
            .unwrap();
        let reader = ColumnGroupReader::open_path(&path).unwrap();
        let row = reader
            .read_row(10, &[PropertyId(3), PropertyId(1), PropertyId(777)])
            .unwrap();
        assert_eq!(row[0], Value::Null); // row 10 % 5 == 0 -> null
        assert_eq!(row[1], Value::String("name-10".to_string()));
        assert_eq!(row[2], Value::Null); // property absent from the group
        let row = reader.read_row(11, &[PropertyId(3)]).unwrap();
        assert_eq!(row[0], Value::Int(11 * 7 - 100));
        assert!(matches!(
            reader.read_row(200, &[PropertyId(3)]),
            Err(ColumnGroupError::RowOutOfRange { .. })
        ));
        let selected = reader
            .read_column(PropertyId(1), Some(&[199, 0, 17]))
            .unwrap();
        assert_eq!(
            selected,
            vec![
                Value::String(format!("name-{}", 199 % 17)),
                Value::String("name-0".to_string()),
                Value::String("name-0".to_string()),
            ]
        );
        assert!(matches!(
            reader.read_column(PropertyId(1), Some(&[200])),
            Err(ColumnGroupError::RowOutOfRange { .. })
        ));
        fs::remove_file(path).unwrap();
    }
}
