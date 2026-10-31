//! Own summary allocations through the last shared projection. Scalar values
//! are copied once after admission; input property maps remain borrowed.

use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointDecodeContext};
use crate::projection::predicate_checkpoint::decode::MapMemory;
use crate::scan::ScanScalar;
use std::cell::RefCell;
use std::sync::Arc;

pub(super) const EXACT_CURSOR_ALLOCATION_BYTES: usize = 2 * 1024;

#[derive(Debug)]
struct OwnedSummary {
    value: SegmentSummary,
    // Data is destroyed before its allocation inventory.
    memory: CheckpointAllocationOwner,
}

#[derive(Debug, Clone)]
pub(super) struct SharedSummary(Arc<OwnedSummary>);

impl std::ops::Deref for SharedSummary {
    type Target = SegmentSummary;
    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

impl PartialEq<SegmentSummary> for SharedSummary {
    fn eq(&self, other: &SegmentSummary) -> bool {
        self.0.value == *other
    }
}

impl PartialEq for SharedSummary {
    fn eq(&self, other: &Self) -> bool {
        self.0.value == other.0.value
    }
}

impl PartialEq<SharedSummary> for SegmentSummary {
    fn eq(&self, other: &SharedSummary) -> bool {
        *self == other.0.value
    }
}

impl SharedSummary {
    pub(super) fn unadmitted(value: SegmentSummary) -> Self {
        Self(Arc::new(OwnedSummary {
            value,
            memory: Default::default(),
        }))
    }

    pub(super) fn unique_mut(&mut self) -> &mut SegmentSummary {
        &mut Arc::get_mut(&mut self.0)
            .expect("descriptor decoder owns its summary")
            .value
    }

    pub(super) fn into_unadmitted(self) -> SegmentSummary {
        let owned = Arc::try_unwrap(self.0).expect("descriptor decoder owns its summary");
        assert!(
            owned.memory.is_empty(),
            "admitted summary cannot detach its inventory"
        );
        owned.value
    }
}

pub(super) fn build<R: SourceScanProperties>(
    segment_id: u64,
    rows: &[R],
    work: &CheckpointWorkContext,
) -> Result<SharedSummary> {
    let owned = CheckpointDecodeContext {
        work: work.clone(),
        memory: RefCell::default(),
    };
    let temporary = CheckpointDecodeContext {
        work: work.clone(),
        memory: RefCell::default(),
    };
    let mut value = SegmentSummary::new(segment_id, rows.len() as u64);
    let mut fields = BTreeSet::<&str>::new();
    let mut field_names = MapMemory::<()>::default();
    for row in rows {
        for field in row.properties().keys() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            if !fields.contains(field.as_str()) {
                field_names.before_insert(fields.len(), &temporary)?;
                fields.insert(field);
            }
            unit.finish();
        }
    }
    let mut field_memory = MapMemory::<FieldSummary>::default();
    for field in fields {
        field_memory.before_insert(value.fields.len(), &owned)?;
        let field_value = field_summary(rows, field, &owned)?;
        let name = owned.string(field)?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        value.insert_field(name, field_value);
        unit.finish();
    }
    // Reserve the shared cell before allocation, including Arc counters.
    owned.reserve(std::mem::size_of::<OwnedSummary>() + 2 * std::mem::size_of::<usize>())?;
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let output = SharedSummary(Arc::new(OwnedSummary {
        value,
        memory: owned.memory.into_inner(),
    }));
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(output)
}

// Match ScanScalar ordering while borrowing the captured rows' strings. The
// fixed segment limit bounds the position bitmap and the temporary key tree.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScalarRef<'a> {
    Bool(bool),
    Int(i64),
    Float(u64),
    String(&'a str),
    Uuid(hawdb_core::Uuid),
}

impl<'a> ScalarRef<'a> {
    fn from_value(value: &'a Value) -> Option<Self> {
        match value {
            Value::Bool(value) => Some(Self::Bool(*value)),
            Value::Int(value) => Some(Self::Int(*value)),
            Value::Float(value) if value.is_finite() => Some(Self::Float(value.to_bits())),
            Value::String(value) => Some(Self::String(value)),
            Value::Uuid(value) => Some(Self::Uuid(*value)),
            Value::Null
            | Value::Float(_)
            | Value::Binary(_)
            | Value::Vector(_)
            | Value::List(_)
            | Value::Map(_)
            | Value::Date(_)
            | Value::LocalTime(_)
            | Value::LocalDateTime(_)
            | Value::DateTime(_)
            | Value::Duration(_) => None,
        }
    }

    fn owned(self, work: &CheckpointDecodeContext) -> Result<ScanScalar> {
        Ok(match self {
            Self::Bool(value) => ScanScalar::Bool(value),
            Self::Int(value) => ScanScalar::Int(value),
            Self::Float(value) => ScanScalar::Float(value),
            Self::String(value) => ScanScalar::String(work.string(value)?),
            Self::Uuid(value) => ScanScalar::Uuid(value),
        })
    }
}

fn field_summary<R: SourceScanProperties>(
    rows: &[R],
    field: &str,
    work: &CheckpointDecodeContext,
) -> Result<FieldSummary> {
    if rows.len() > SOURCE_SCAN_TARGET_ROWS {
        return Err(HawDBError::StorageIntegrity(
            "Source summary exceeds the fixed segment row limit".into(),
        ));
    }
    let temporary = owned::context(work);
    let mut inputs = BTreeMap::<ScalarRef<'_>, u128>::new();
    // ScalarRef is no larger than String; u128 also accounts its alignment.
    let mut input_memory = MapMemory::<u128>::default();
    let mut present_count = 0u64;
    let mut null_count = 0u64;
    let mut dictionary = EnumDictionaryStats {
        values: BTreeSet::new(),
        complete: true,
    };
    // ScanScalar is at most a String plus one u64 on the pinned toolchain.
    let mut dictionary_memory = MapMemory::<u64>::default();
    let mut exact = BTreeMap::<ScanScalar, roaring::RoaringTreemap>::new();
    let mut exact_memory = MapMemory::<(roaring::RoaringTreemap, u64)>::default();
    let mut numeric_min = None::<f64>;
    let mut numeric_max = None::<f64>;
    let mut datetime_min = None::<i64>;
    let mut datetime_max = None::<i64>;
    for (row_id, row) in rows.iter().enumerate() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let Some(input) = row.properties().get(field) else {
            unit.finish();
            continue;
        };
        present_count += 1;
        if matches!(input, Value::Null) {
            null_count += 1;
            unit.finish();
            continue;
        }
        if let Some(number) = precise_numeric_value(input) {
            numeric_min = Some(numeric_min.map_or(number, |min| min.min(number)));
            numeric_max = Some(numeric_max.map_or(number, |max| max.max(number)));
        }
        if let Value::String(text) = input
            && let Some(time) = DateTimeMinMax::parse_rfc3339(text)
        {
            datetime_min = Some(datetime_min.map_or(time, |min| min.min(time)));
            datetime_max = Some(datetime_max.map_or(time, |max| max.max(time)));
        }
        let scalar_input = ScalarRef::from_value(input);
        unit.finish();
        let Some(key) = scalar_input else {
            continue;
        };
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        if let Some(positions) = inputs.get_mut(&key) {
            *positions |= 1u128 << row_id;
            unit.finish();
            continue;
        }
        unit.finish();
        input_memory.before_insert(inputs.len(), &temporary)?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        inputs.insert(key, 1u128 << row_id);
        unit.finish();
    }
    for (key, mut positions) in inputs {
        dictionary_memory.before_insert(dictionary.values.len(), work)?;
        let item = key.owned(work)?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        dictionary.values.insert(item);
        unit.finish();
        if UNIQUE_KEY_FIELDS.contains(&field) {
            exact_memory.before_insert(exact.len(), work)?;
            let key = key.owned(work)?;
            // Roaring 0.10.12: positions 0..127 use one treemap leaf, one
            // bitmap container Vec and an array, never an 8 KiB bitmap.
            // This bound includes replacement overlap and allocator layout.
            work.reserve(EXACT_CURSOR_ALLOCATION_BYTES)?;
            let mut cursor = roaring::RoaringTreemap::new();
            while positions != 0 {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                cursor.insert(u64::from(positions.trailing_zeros()));
                positions &= positions - 1;
                unit.finish();
            }
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            exact.insert(key, cursor);
            unit.finish();
        }
    }
    let count = rows.len() as u64;
    let mut summary = FieldSummary::new(count)
        .with_presence_counts(present_count, null_count, count - present_count)
        .expect("source scan field counts must match rows");
    if let (Some(min), Some(max)) = (numeric_min, numeric_max) {
        summary = summary
            .with_numeric_min_max(min, max)
            .expect("valid Source numeric range");
    }
    if let (Some(min), Some(max)) = (datetime_min, datetime_max) {
        summary = summary
            .with_datetime_min_max(min, max)
            .expect("valid Source datetime range");
    }
    if !dictionary.values.is_empty() {
        summary.enum_dictionary = Some(dictionary);
    }
    summary.exact_values = exact;
    Ok(summary)
}
