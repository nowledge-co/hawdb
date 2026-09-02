//! Typed columnar batches used by vectorized executor fragments.

use skein_core::{LogicalType, Result, SkeinError, Value, ValueRef};
use skein_plan::ComparisonOp;
use skein_storage::{RelationalKey, RelationalValue};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int64,
    Float64,
    Utf8,
    NodeId,
    RelationalRowLocator,
    Dynamic,
}

/// The semantic value type or executor-private role assigned to one slot.
///
/// Query-visible values use the shared [`LogicalType`]. Physical identifiers
/// and row locators remain explicit executor roles and cannot accidentally
/// escape as logical schema types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    Logical(LogicalType),
    NodeId,
    RelationalRowLocator,
}

impl SlotType {
    pub const fn logical(logical_type: LogicalType) -> Self {
        Self::Logical(logical_type)
    }

    pub const fn logical_type(self) -> Option<LogicalType> {
        match self {
            Self::Logical(logical_type) => Some(logical_type),
            Self::NodeId => Some(LogicalType::Int64),
            Self::RelationalRowLocator => None,
        }
    }

    pub const fn accepts(self, column_type: ColumnType) -> bool {
        match (self, column_type) {
            (
                Self::Logical(LogicalType::Any),
                ColumnType::Bool
                | ColumnType::Int64
                | ColumnType::Float64
                | ColumnType::Utf8
                | ColumnType::Dynamic,
            )
            | (Self::Logical(LogicalType::Boolean), ColumnType::Bool)
            | (Self::Logical(LogicalType::Int64), ColumnType::Int64)
            | (Self::Logical(LogicalType::Float64), ColumnType::Float64)
            | (Self::Logical(LogicalType::String | LogicalType::Text), ColumnType::Utf8)
            | (Self::NodeId, ColumnType::NodeId)
            | (Self::RelationalRowLocator, ColumnType::RelationalRowLocator) => true,
            (Self::Logical(logical_type), ColumnType::Dynamic) => {
                !matches!(logical_type, LogicalType::Binary)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowLocator {
    table_id: u32,
    primary_key: RelationalKey,
}

impl RelationalRowLocator {
    pub fn new(table_id: u32, primary_key: RelationalKey) -> Self {
        Self {
            table_id,
            primary_key,
        }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn primary_key(&self) -> &RelationalKey {
        &self.primary_key
    }

    pub fn allocated_bytes(&self) -> usize {
        self.primary_key
            .0
            .capacity()
            .saturating_mul(std::mem::size_of::<RelationalValue>())
            .saturating_add(
                self.primary_key
                    .0
                    .iter()
                    .map(|value| match value {
                        RelationalValue::Text(value) => value.capacity(),
                        RelationalValue::Bytea(value) => value.capacity(),
                        RelationalValue::Null
                        | RelationalValue::Boolean(_)
                        | RelationalValue::BigInt(_)
                        | RelationalValue::DoublePrecision(_)
                        | RelationalValue::Uuid(_)
                        | RelationalValue::Overflow(_) => 0,
                    })
                    .sum::<usize>(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotDescriptor {
    pub id: SlotId,
    pub name: String,
    pub slot_type: SlotType,
}

impl SlotDescriptor {
    pub const fn logical_type(&self) -> Option<LogicalType> {
        self.slot_type.logical_type()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingSchema {
    slots: Arc<[SlotDescriptor]>,
}

impl BindingSchema {
    pub fn try_new(slots: Vec<SlotDescriptor>) -> Result<Self> {
        for (index, slot) in slots.iter().enumerate() {
            if slot.id.0 as usize != index {
                return Err(SkeinError::Execution(format!(
                    "columnar schema slot ids must be dense: expected {index}, got {}",
                    slot.id.0
                )));
            }
        }
        Ok(Self {
            slots: slots.into(),
        })
    }

    pub fn slots(&self) -> &[SlotDescriptor] {
        &self.slots
    }

    pub fn slot(&self, id: SlotId) -> Option<&SlotDescriptor> {
        self.slots.get(id.0 as usize)
    }

    pub fn slot_by_name(&self, name: &str) -> Option<&SlotDescriptor> {
        self.slots.iter().find(|slot| slot.name == name)
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validity {
    All { len: usize },
    Bitmap { len: usize, words: Arc<[u64]> },
}

impl Validity {
    pub fn all(len: usize) -> Self {
        Self::All { len }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::All { len } | Self::Bitmap { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_valid(&self, row: usize) -> bool {
        if row >= self.len() {
            return false;
        }
        match self {
            Self::All { .. } => true,
            Self::Bitmap { words, .. } => words
                .get(row / u64::BITS as usize)
                .is_some_and(|word| word & (1u64 << (row % u64::BITS as usize)) != 0),
        }
    }

    pub fn valid_count(&self) -> usize {
        match self {
            Self::All { len } => *len,
            Self::Bitmap { len, words } => count_bitmap_rows(words, *len),
        }
    }

    pub fn view(&self) -> ValidityView<'_> {
        match self {
            Self::All { len } => ValidityView::All { len: *len },
            Self::Bitmap { len, words } => ValidityView::Bitmap { len: *len, words },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ValidityView<'a> {
    All { len: usize },
    Bitmap { len: usize, words: &'a [u64] },
}

impl ValidityView<'_> {
    pub fn len(self) -> usize {
        match self {
            Self::All { len } | Self::Bitmap { len, .. } => len,
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub fn is_valid(self, row: usize) -> bool {
        if row >= self.len() {
            return false;
        }
        match self {
            Self::All { .. } => true,
            Self::Bitmap { words, .. } => words
                .get(row / u64::BITS as usize)
                .is_some_and(|word| word & (1u64 << (row % u64::BITS as usize)) != 0),
        }
    }

    pub fn valid_count(self) -> usize {
        match self {
            Self::All { len } => len,
            Self::Bitmap { len, words } => count_bitmap_rows(words, len),
        }
    }

    pub fn to_owned(self) -> Validity {
        match self {
            Self::All { len } => Validity::All { len },
            Self::Bitmap { len, words } => Validity::Bitmap {
                len,
                words: words.to_vec().into(),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct ValidityBuilder {
    len: usize,
    words: Vec<u64>,
    word_capacity: usize,
}

impl ValidityBuilder {
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            len: 0,
            words: Vec::new(),
            word_capacity: rows.div_ceil(u64::BITS as usize),
        }
    }

    pub fn push(&mut self, valid: bool) {
        let word_index = self.len / u64::BITS as usize;
        let bit_index = self.len % u64::BITS as usize;
        if self.words.is_empty() {
            if !valid {
                if self.words.capacity() < self.word_capacity {
                    self.words.reserve(self.word_capacity);
                }
                self.words.resize(word_index + 1, u64::MAX);
                self.words[word_index] = if bit_index == 0 {
                    0
                } else {
                    (1u64 << bit_index) - 1
                };
            }
        } else {
            if word_index == self.words.len() {
                self.words.push(0);
            }
            if valid {
                self.words[word_index] |= 1u64 << bit_index;
            }
        }
        self.len = self.len.saturating_add(1);
    }

    pub fn clear(&mut self) {
        self.len = 0;
        self.words.clear();
    }

    pub fn view(&self) -> ValidityView<'_> {
        if self.words.is_empty() {
            ValidityView::All { len: self.len }
        } else {
            ValidityView::Bitmap {
                len: self.len,
                words: &self.words,
            }
        }
    }

    pub fn finish(self) -> Validity {
        if self.words.is_empty() {
            Validity::All { len: self.len }
        } else {
            Validity::Bitmap {
                len: self.len,
                words: self.words.into(),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnVector {
    Bool {
        values: Arc<[u8]>,
        validity: Validity,
    },
    Int64 {
        values: Arc<[i64]>,
        validity: Validity,
    },
    Float64 {
        values: Arc<[f64]>,
        validity: Validity,
    },
    Utf8 {
        values: Arc<[String]>,
        validity: Validity,
    },
    NodeId(Arc<[u64]>),
    RelationalRowLocator(Arc<[RelationalRowLocator]>),
    Dynamic(Arc<[Value]>),
}

impl ColumnVector {
    pub fn boolean(values: Vec<bool>, validity: Validity) -> Result<Self> {
        Self::boolean_bytes(values.into_iter().map(u8::from).collect(), validity)
    }

    pub fn boolean_bytes(values: Vec<u8>, validity: Validity) -> Result<Self> {
        ensure_column_len("Bool", values.len(), validity.len())?;
        if values.iter().any(|value| *value > 1) {
            return Err(SkeinError::Execution(
                "Bool column contains a value other than 0 or 1".to_string(),
            ));
        }
        Ok(Self::Bool {
            values: values.into(),
            validity,
        })
    }

    pub fn int64(values: Vec<i64>, validity: Validity) -> Result<Self> {
        ensure_column_len("Int64", values.len(), validity.len())?;
        Ok(Self::Int64 {
            values: values.into(),
            validity,
        })
    }

    pub fn float64(values: Vec<f64>, validity: Validity) -> Result<Self> {
        ensure_column_len("Float64", values.len(), validity.len())?;
        Ok(Self::Float64 {
            values: values.into(),
            validity,
        })
    }

    pub fn node_ids(values: Vec<u64>) -> Self {
        Self::NodeId(values.into())
    }

    pub fn relational_row_locators(values: Vec<RelationalRowLocator>) -> Self {
        Self::RelationalRowLocator(values.into())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bool { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len(),
            Self::Float64 { values, .. } => values.len(),
            Self::Utf8 { values, .. } => values.len(),
            Self::NodeId(values) => values.len(),
            Self::RelationalRowLocator(values) => values.len(),
            Self::Dynamic(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn column_type(&self) -> ColumnType {
        match self {
            Self::Bool { .. } => ColumnType::Bool,
            Self::Int64 { .. } => ColumnType::Int64,
            Self::Float64 { .. } => ColumnType::Float64,
            Self::Utf8 { .. } => ColumnType::Utf8,
            Self::NodeId(_) => ColumnType::NodeId,
            Self::RelationalRowLocator(_) => ColumnType::RelationalRowLocator,
            Self::Dynamic(_) => ColumnType::Dynamic,
        }
    }

    pub fn is_valid(&self, row: usize) -> bool {
        match self {
            Self::Bool { validity, .. }
            | Self::Int64 { validity, .. }
            | Self::Float64 { validity, .. }
            | Self::Utf8 { validity, .. } => validity.is_valid(row),
            Self::NodeId(values) => row < values.len(),
            Self::RelationalRowLocator(values) => row < values.len(),
            Self::Dynamic(values) => values.get(row).is_some_and(|value| *value != Value::Null),
        }
    }

    pub fn value_ref(&self, row: usize) -> Option<ValueRef<'_>> {
        match self {
            Self::Bool { values, validity } if validity.is_valid(row) => {
                values.get(row).map(|value| ValueRef::Bool(*value != 0))
            }
            Self::Int64 { values, validity } if validity.is_valid(row) => {
                values.get(row).copied().map(ValueRef::Int)
            }
            Self::Float64 { values, validity } if validity.is_valid(row) => {
                values.get(row).copied().map(ValueRef::Float)
            }
            Self::Utf8 { values, validity } if validity.is_valid(row) => {
                values.get(row).map(|value| ValueRef::String(value))
            }
            Self::Bool { .. } | Self::Int64 { .. } | Self::Float64 { .. } | Self::Utf8 { .. }
                if row < self.len() =>
            {
                Some(ValueRef::Null)
            }
            Self::Bool { .. } | Self::Int64 { .. } | Self::Float64 { .. } | Self::Utf8 { .. } => {
                None
            }
            Self::NodeId(values) => values.get(row).map(|value| ValueRef::Int(*value as i64)),
            Self::RelationalRowLocator(_) => None,
            Self::Dynamic(values) => values.get(row).map(Value::as_ref),
        }
    }

    pub fn value(&self, row: usize) -> Option<Value> {
        self.value_ref(row).map(ValueRef::to_owned_value)
    }

    pub fn relational_row_locator(&self, row: usize) -> Option<&RelationalRowLocator> {
        let Self::RelationalRowLocator(values) = self else {
            return None;
        };
        values.get(row)
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        let validity_bytes = match self {
            Self::Bool { validity, .. }
            | Self::Int64 { validity, .. }
            | Self::Float64 { validity, .. }
            | Self::Utf8 { validity, .. } => match validity {
                Validity::All { .. } => 0,
                Validity::Bitmap { words, .. } => words.len() * std::mem::size_of::<u64>(),
            },
            Self::NodeId(_) | Self::RelationalRowLocator(_) | Self::Dynamic(_) => 0,
        };
        validity_bytes.saturating_add(match self {
            Self::Bool { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len() * std::mem::size_of::<i64>(),
            Self::Float64 { values, .. } => values.len() * std::mem::size_of::<f64>(),
            Self::Utf8 { values, .. } => values.iter().fold(
                values.len() * std::mem::size_of::<String>(),
                |total, value| total.saturating_add(value.len()),
            ),
            Self::NodeId(values) => values.len() * std::mem::size_of::<u64>(),
            Self::RelationalRowLocator(values) => values.iter().fold(
                values
                    .len()
                    .saturating_mul(std::mem::size_of::<RelationalRowLocator>()),
                |total, locator| total.saturating_add(locator.allocated_bytes()),
            ),
            Self::Dynamic(values) => values.len() * std::mem::size_of::<Value>(),
        })
    }
}

fn ensure_column_len(kind: &str, values: usize, validity: usize) -> Result<()> {
    if values != validity {
        return Err(SkeinError::Execution(format!(
            "{kind} column has {values} values but {validity} validity entries"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    All {
        len: usize,
    },
    Bitmap {
        len: usize,
        words: Arc<[u64]>,
        selected: usize,
    },
    Indices {
        len: usize,
        rows: Arc<[u32]>,
    },
}

impl Selection {
    pub fn all(len: usize) -> Self {
        Self::All { len }
    }

    pub fn none(len: usize) -> Self {
        Self::Indices {
            len,
            rows: Arc::from([]),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::All { len } | Self::Bitmap { len, .. } | Self::Indices { len, .. } => *len,
        }
    }

    pub fn selected_count(&self) -> usize {
        match self {
            Self::All { len } => *len,
            Self::Bitmap { selected, .. } => *selected,
            Self::Indices { rows, .. } => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.selected_count() == 0
    }

    pub fn iter(&self) -> SelectionIter<'_> {
        SelectionIter {
            selection: self,
            cursor: 0,
        }
    }

    pub fn limit(&self, offset: usize, max_rows: usize) -> Self {
        let mut output = SelectionBuilder::new(self.len());
        for row in self.iter().skip(offset).take(max_rows) {
            output.select(row);
        }
        output.finish()
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        match self {
            Self::All { .. } => 0,
            Self::Bitmap { words, .. } => words.len() * std::mem::size_of::<u64>(),
            Self::Indices { rows, .. } => rows.len() * std::mem::size_of::<u32>(),
        }
    }
}

struct SelectionBuilder {
    len: usize,
    selected: usize,
    sparse_rows: Vec<u32>,
    bitmap_words: Option<Vec<u64>>,
}

impl SelectionBuilder {
    fn new(len: usize) -> Self {
        Self {
            len,
            selected: 0,
            sparse_rows: Vec::new(),
            bitmap_words: (len > u32::MAX as usize)
                .then(|| vec![0; len.div_ceil(u64::BITS as usize)]),
        }
    }

    fn select(&mut self, row: usize) {
        self.selected = self.selected.saturating_add(1);
        if let Some(words) = &mut self.bitmap_words {
            words[row / u64::BITS as usize] |= 1u64 << (row % u64::BITS as usize);
            return;
        }

        self.sparse_rows.push(row as u32);
        if self.selected.saturating_mul(8) > self.len {
            let rows = std::mem::take(&mut self.sparse_rows);
            let mut words = vec![0; self.len.div_ceil(u64::BITS as usize)];
            for row in rows {
                let row = row as usize;
                words[row / u64::BITS as usize] |= 1u64 << (row % u64::BITS as usize);
            }
            self.bitmap_words = Some(words);
        }
    }

    fn finish(self) -> Selection {
        if self.selected == self.len {
            return Selection::all(self.len);
        }
        if self.selected == 0 {
            return Selection::none(self.len);
        }
        match self.bitmap_words {
            Some(words) => Selection::Bitmap {
                len: self.len,
                words: words.into(),
                selected: self.selected,
            },
            None => Selection::Indices {
                len: self.len,
                rows: self.sparse_rows.into(),
            },
        }
    }
}

pub struct SelectionIter<'a> {
    selection: &'a Selection,
    cursor: usize,
}

impl Iterator for SelectionIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.selection {
            Selection::All { len } => {
                let row = self.cursor;
                if row == *len {
                    return None;
                }
                self.cursor = self.cursor.saturating_add(1);
                Some(row)
            }
            Selection::Indices { rows, .. } => {
                let row = rows.get(self.cursor).copied()? as usize;
                self.cursor = self.cursor.saturating_add(1);
                Some(row)
            }
            Selection::Bitmap { len, words, .. } => {
                while self.cursor < *len {
                    let row = self.cursor;
                    self.cursor = self.cursor.saturating_add(1);
                    if words[row / u64::BITS as usize] & (1u64 << (row % u64::BITS as usize)) != 0 {
                        return Some(row);
                    }
                }
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnarBatch {
    schema: Arc<BindingSchema>,
    columns: Vec<Arc<ColumnVector>>,
    selection: Selection,
    row_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnarRowRef<'a> {
    batch: &'a ColumnarBatch,
    row: usize,
}

impl<'a> ColumnarRowRef<'a> {
    pub fn row_index(self) -> usize {
        self.row
    }

    pub fn schema(self) -> &'a BindingSchema {
        self.batch.schema()
    }

    pub fn value(self, slot: SlotId) -> Option<ValueRef<'a>> {
        self.batch.value_ref(slot, self.row)
    }

    pub fn get(self, name: &str) -> Option<ValueRef<'a>> {
        let slot = self.schema().slot_by_name(name)?;
        self.value(slot.id)
    }

    pub fn column(self, index: usize) -> Option<(&'a str, ValueRef<'a>)> {
        let slot = self.schema().slots().get(index)?;
        self.value(slot.id).map(|value| (slot.name.as_str(), value))
    }
}

impl ColumnarBatch {
    pub fn try_new(schema: Arc<BindingSchema>, columns: Vec<Arc<ColumnVector>>) -> Result<Self> {
        if schema.len() != columns.len() {
            return Err(SkeinError::Execution(format!(
                "columnar batch has {} slots but {} columns",
                schema.len(),
                columns.len()
            )));
        }
        let row_count = columns.first().map_or(0, |column| column.len());
        for (slot, column) in schema.slots().iter().zip(&columns) {
            if column.len() != row_count {
                return Err(SkeinError::Execution(format!(
                    "columnar slot '{}' has {} rows, expected {row_count}",
                    slot.name,
                    column.len()
                )));
            }
            if !slot.slot_type.accepts(column.column_type()) {
                return Err(SkeinError::Execution(format!(
                    "columnar slot '{}' expects {:?}, got {:?}",
                    slot.name,
                    slot.slot_type,
                    column.column_type()
                )));
            }
        }
        Ok(Self {
            schema,
            columns,
            selection: Selection::all(row_count),
            row_count,
        })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn selected_count(&self) -> usize {
        self.selection.selected_count()
    }

    pub fn schema(&self) -> &BindingSchema {
        &self.schema
    }

    pub fn column(&self, slot: SlotId) -> Option<&Arc<ColumnVector>> {
        self.columns.get(slot.0 as usize)
    }

    pub fn value_ref(&self, slot: SlotId, row: usize) -> Option<ValueRef<'_>> {
        self.column(slot)?.value_ref(row)
    }

    pub fn selection(&self) -> &Selection {
        &self.selection
    }

    pub fn rows(&self) -> impl Iterator<Item = ColumnarRowRef<'_>> {
        self.selection
            .iter()
            .map(|row| ColumnarRowRef { batch: self, row })
    }

    pub fn with_selection(mut self, selection: Selection) -> Result<Self> {
        if selection.len() != self.row_count {
            return Err(SkeinError::Execution(format!(
                "columnar selection has {} rows, expected {}",
                selection.len(),
                self.row_count
            )));
        }
        self.selection = selection;
        Ok(self)
    }

    pub fn project(&self, slots: &[SlotId]) -> Result<Self> {
        let mut descriptors = Vec::with_capacity(slots.len());
        let mut columns = Vec::with_capacity(slots.len());
        for (output_index, slot) in slots.iter().copied().enumerate() {
            let descriptor = self.schema.slot(slot).ok_or_else(|| {
                SkeinError::Execution(format!("unknown columnar slot {}", slot.0))
            })?;
            descriptors.push(SlotDescriptor {
                id: SlotId(output_index as u32),
                name: descriptor.name.clone(),
                slot_type: descriptor.slot_type,
            });
            columns.push(Arc::clone(
                self.column(slot).expect("schema and columns align"),
            ));
        }
        Ok(Self {
            schema: Arc::new(BindingSchema::try_new(descriptors)?),
            columns,
            selection: self.selection.clone(),
            row_count: self.row_count,
        })
    }

    pub fn filter_numeric(
        &self,
        slot: SlotId,
        op: ComparisonOp,
        expected: NumericLiteral,
    ) -> Result<Self> {
        let column = self.column(slot).ok_or_else(|| {
            SkeinError::Execution(format!("unknown columnar filter slot {}", slot.0))
        })?;
        let selection = filter_numeric_column(column, &self.selection, op, expected)?;
        self.clone().with_selection(selection)
    }

    pub fn filter_boolean(&self, slot: SlotId, expected: bool) -> Result<Self> {
        let column = self.column(slot).ok_or_else(|| {
            SkeinError::Execution(format!("unknown columnar filter slot {}", slot.0))
        })?;
        let selection = filter_boolean_column(column, &self.selection, expected)?;
        self.clone().with_selection(selection)
    }

    pub fn limit(&self, offset: usize, max_rows: usize) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            columns: self.columns.clone(),
            selection: self.selection.limit(offset, max_rows),
            row_count: self.row_count,
        }
    }

    pub fn count_selected(&self) -> usize {
        self.selection.selected_count()
    }

    pub fn count_valid(&self, slot: SlotId) -> Result<usize> {
        let column = self.column(slot).ok_or_else(|| {
            SkeinError::Execution(format!("unknown columnar aggregate slot {}", slot.0))
        })?;
        Ok(self
            .selection
            .iter()
            .filter(|row| column.is_valid(*row))
            .count())
    }

    pub fn sum_int64(&self, slot: SlotId) -> Result<Option<i64>> {
        let column = self.column(slot).ok_or_else(|| {
            SkeinError::Execution(format!("unknown columnar aggregate slot {}", slot.0))
        })?;
        let ColumnVector::Int64 { values, validity } = column.as_ref() else {
            return Err(SkeinError::Execution(format!(
                "columnar SUM requires Int64, got {:?}",
                column.column_type()
            )));
        };
        let mut sum = None::<i64>;
        for row in self.selection.iter() {
            if validity.is_valid(row) {
                sum =
                    Some(sum.unwrap_or(0).checked_add(values[row]).ok_or_else(|| {
                        SkeinError::Execution("columnar Int64 SUM overflow".into())
                    })?);
            }
        }
        Ok(sum)
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        let schema_bytes = self.schema.slots().iter().fold(
            self.schema.len() * std::mem::size_of::<SlotDescriptor>(),
            |total, slot| total.saturating_add(slot.name.len()),
        );
        self.columns
            .iter()
            .fold(schema_bytes, |total, column| {
                total.saturating_add(column.estimated_memory_bytes())
            })
            .saturating_add(self.selection.estimated_memory_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumericLiteral {
    Int(i64),
    Float(f64),
}

impl NumericLiteral {
    pub fn from_value(value: &Value) -> Option<Self> {
        Self::from_value_ref(value.as_ref())
    }

    pub fn from_value_ref(value: ValueRef<'_>) -> Option<Self> {
        match value {
            ValueRef::Int(value) => Some(Self::Int(value)),
            ValueRef::Float(value) => Some(Self::Float(value)),
            _ => None,
        }
    }
}

pub fn filter_numeric_column(
    column: &ColumnVector,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    if column.len() != input.len() {
        return Err(SkeinError::Execution(format!(
            "numeric filter column has {} rows but input selection has {}",
            column.len(),
            input.len()
        )));
    }
    match column {
        ColumnVector::Int64 { values, validity } => {
            filter_int64_values(values, validity, input, op, expected)
        }
        ColumnVector::Float64 { values, validity } => {
            filter_float64_values(values, validity, input, op, expected)
        }
        other => Err(SkeinError::Execution(format!(
            "numeric filter requires Int64 or Float64, got {:?}",
            other.column_type()
        ))),
    }
}

pub fn filter_boolean_column(
    column: &ColumnVector,
    input: &Selection,
    expected: bool,
) -> Result<Selection> {
    if column.len() != input.len() {
        return Err(SkeinError::Execution(format!(
            "boolean filter column has {} rows but input selection has {}",
            column.len(),
            input.len()
        )));
    }
    let ColumnVector::Bool { values, validity } = column else {
        return Err(SkeinError::Execution(format!(
            "boolean filter requires Bool, got {:?}",
            column.column_type()
        )));
    };
    let expected = u8::from(expected);
    let mut output = SelectionBuilder::new(input.len());
    for row in input.iter() {
        if validity.is_valid(row) && values[row] == expected {
            output.select(row);
        }
    }
    Ok(output.finish())
}

pub fn filter_int64_values(
    values: &[i64],
    validity: &Validity,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    filter_int64_values_view(values, validity.view(), input, op, expected)
}

pub fn filter_int64_values_view(
    values: &[i64],
    validity: ValidityView<'_>,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    filter_numeric_values(values, validity, input, |actual| {
        int64_value_matches(actual, op, expected)
    })
}

pub fn filter_float64_values(
    values: &[f64],
    validity: &Validity,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    filter_float64_values_view(values, validity.view(), input, op, expected)
}

pub fn filter_float64_values_view(
    values: &[f64],
    validity: ValidityView<'_>,
    input: &Selection,
    op: ComparisonOp,
    expected: NumericLiteral,
) -> Result<Selection> {
    filter_numeric_values(values, validity, input, |actual| {
        float64_value_matches(actual, op, expected)
    })
}

pub fn select_int64_values_view(
    values: &[i64],
    validity: ValidityView<'_>,
    op: ComparisonOp,
    expected: NumericLiteral,
    selected_rows: &mut Vec<u32>,
) -> Result<()> {
    select_numeric_values(values, validity, selected_rows, |actual| {
        int64_value_matches(actual, op, expected)
    })
}

pub fn select_float64_values_view(
    values: &[f64],
    validity: ValidityView<'_>,
    op: ComparisonOp,
    expected: NumericLiteral,
    selected_rows: &mut Vec<u32>,
) -> Result<()> {
    select_numeric_values(values, validity, selected_rows, |actual| {
        float64_value_matches(actual, op, expected)
    })
}

fn select_numeric_values<T: Copy>(
    values: &[T],
    validity: ValidityView<'_>,
    selected_rows: &mut Vec<u32>,
    mut matches: impl FnMut(T) -> bool,
) -> Result<()> {
    if values.len() != validity.len() {
        return Err(SkeinError::Execution(format!(
            "numeric selection has {} values and {} validity entries",
            values.len(),
            validity.len()
        )));
    }
    if values.len() > u32::MAX as usize {
        return Err(SkeinError::Execution(format!(
            "numeric selection batch has {} rows, exceeding the u32 row index limit",
            values.len()
        )));
    }
    selected_rows.clear();
    if selected_rows.capacity() < values.len() {
        selected_rows.reserve(values.len());
    }
    match validity {
        ValidityView::All { .. } => {
            for (row, value) in values.iter().copied().enumerate() {
                if matches(value) {
                    selected_rows.push(row as u32);
                }
            }
        }
        ValidityView::Bitmap { .. } => {
            for (row, value) in values.iter().copied().enumerate() {
                if validity.is_valid(row) && matches(value) {
                    selected_rows.push(row as u32);
                }
            }
        }
    }
    Ok(())
}

fn filter_numeric_values<T: Copy>(
    values: &[T],
    validity: ValidityView<'_>,
    input: &Selection,
    mut matches: impl FnMut(T) -> bool,
) -> Result<Selection> {
    if values.len() != validity.len() || values.len() != input.len() {
        return Err(SkeinError::Execution(format!(
            "numeric filter has {} values, {} validity entries, and {} selected input rows",
            values.len(),
            validity.len(),
            input.len()
        )));
    }
    let mut output = SelectionBuilder::new(input.len());
    for row in input.iter() {
        if validity.is_valid(row) && matches(values[row]) {
            output.select(row);
        }
    }
    Ok(output.finish())
}

#[inline]
pub fn int64_value_matches(actual: i64, op: ComparisonOp, expected: NumericLiteral) -> bool {
    match expected {
        NumericLiteral::Int(expected) => compare_ordering(actual.cmp(&expected), op),
        NumericLiteral::Float(expected) => {
            compare_ordering((actual as f64).total_cmp(&expected), op)
        }
    }
}

#[inline]
pub fn float64_value_matches(actual: f64, op: ComparisonOp, expected: NumericLiteral) -> bool {
    let expected = match expected {
        NumericLiteral::Int(expected) => expected as f64,
        NumericLiteral::Float(expected) => expected,
    };
    compare_ordering(actual.total_cmp(&expected), op)
}

fn compare_ordering(ordering: std::cmp::Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        ComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        ComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

fn count_bitmap_rows(words: &[u64], len: usize) -> usize {
    words
        .iter()
        .enumerate()
        .fold(0usize, |total, (index, word)| {
            let bits = if index + 1 == words.len() && !len.is_multiple_of(u64::BITS as usize) {
                word & ((1u64 << (len % u64::BITS as usize)) - 1)
            } else {
                *word
            };
            total.saturating_add(bits.count_ones() as usize)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_uses_no_bitmap_for_dense_columns() {
        let mut builder = ValidityBuilder::with_capacity(3);
        builder.push(true);
        builder.push(true);
        builder.push(true);

        assert!(builder.words.is_empty());
        assert_eq!(builder.finish(), Validity::All { len: 3 });
    }

    #[test]
    fn validity_materializes_prior_rows_on_first_null() {
        let mut builder = ValidityBuilder::with_capacity(130);
        for _ in 0..65 {
            builder.push(true);
        }
        builder.push(false);
        builder.push(true);

        let validity = builder.finish();
        assert_eq!(validity.valid_count(), 66);
        assert!(validity.is_valid(64));
        assert!(!validity.is_valid(65));
        assert!(validity.is_valid(66));
    }

    #[test]
    fn value_ref_distinguishes_null_from_out_of_bounds() {
        let column = ColumnVector::int64(
            vec![1, 0],
            Validity::Bitmap {
                len: 2,
                words: Arc::from([0b01]),
            },
        )
        .unwrap();

        assert_eq!(column.value_ref(0), Some(ValueRef::Int(1)));
        assert_eq!(column.value_ref(1), Some(ValueRef::Null));
        assert_eq!(column.value_ref(2), None);
    }

    #[test]
    fn boolean_bytes_reject_non_canonical_values() {
        let error = ColumnVector::boolean_bytes(vec![0, 2], Validity::all(2)).unwrap_err();

        assert!(error.to_string().contains("other than 0 or 1"));
    }

    #[test]
    fn validity_builder_reuses_bitmap_storage_between_batches() {
        let mut builder = ValidityBuilder::with_capacity(130);
        builder.push(false);
        let words_ptr = builder.words.as_ptr();
        let words_capacity = builder.words.capacity();

        builder.clear();
        builder.push(true);
        builder.push(false);

        assert_eq!(builder.words.as_ptr(), words_ptr);
        assert_eq!(builder.words.capacity(), words_capacity);
        let validity = builder.view();
        assert!(!validity.is_empty());
        assert!(validity.is_valid(0));
        assert!(!validity.is_valid(1));
    }

    #[test]
    fn numeric_filter_preserves_null_and_nan_semantics() {
        let mut validity = ValidityBuilder::with_capacity(4);
        validity.push(true);
        validity.push(false);
        validity.push(true);
        validity.push(true);
        let column =
            ColumnVector::float64(vec![1.0, 0.0, f64::NAN, 3.0], validity.finish()).unwrap();

        let selection = filter_numeric_column(
            &column,
            &Selection::all(4),
            ComparisonOp::Gte,
            NumericLiteral::Float(2.0),
        )
        .unwrap();

        assert_eq!(selection.iter().collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn numeric_selection_reuses_row_index_storage() {
        let mut selected_rows = Vec::new();
        select_int64_values_view(
            &[1, 2, 3, 4],
            ValidityView::All { len: 4 },
            ComparisonOp::Gte,
            NumericLiteral::Int(3),
            &mut selected_rows,
        )
        .unwrap();
        assert_eq!(selected_rows, [2, 3]);
        let rows_ptr = selected_rows.as_ptr();

        select_int64_values_view(
            &[5, 6, 7, 8],
            ValidityView::Bitmap {
                len: 4,
                words: &[0b1101],
            },
            ComparisonOp::Lt,
            NumericLiteral::Int(8),
            &mut selected_rows,
        )
        .unwrap();

        assert_eq!(selected_rows, [0, 2]);
        assert_eq!(selected_rows.as_ptr(), rows_ptr);
    }

    #[test]
    fn sparse_filter_uses_index_selection() {
        let column = ColumnVector::int64((0..64).collect(), Validity::all(64)).unwrap();
        let selection = filter_numeric_column(
            &column,
            &Selection::all(64),
            ComparisonOp::Gte,
            NumericLiteral::Int(63),
        )
        .unwrap();

        assert!(matches!(selection, Selection::Indices { .. }));
        assert_eq!(selection.iter().collect::<Vec<_>>(), vec![63]);
    }

    #[test]
    fn dense_filter_uses_bitmap_selection() {
        let column = ColumnVector::int64((0..64).collect(), Validity::all(64)).unwrap();
        let selection = filter_numeric_column(
            &column,
            &Selection::all(64),
            ComparisonOp::Gte,
            NumericLiteral::Int(32),
        )
        .unwrap();

        assert!(matches!(selection, Selection::Bitmap { .. }));
        assert_eq!(selection.selected_count(), 32);
    }

    #[test]
    fn projection_reuses_column_storage() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![SlotDescriptor {
                id: SlotId(0),
                name: "score".to_string(),
                slot_type: SlotType::logical(LogicalType::Int64),
            }])
            .unwrap(),
        );
        let column = Arc::new(ColumnVector::int64(vec![1, 2], Validity::all(2)).unwrap());
        let batch = ColumnarBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&column)]).unwrap();
        let projected = batch.project(&[SlotId(0)]).unwrap();

        assert!(Arc::ptr_eq(projected.column(SlotId(0)).unwrap(), &column));
    }

    #[test]
    fn utf8_value_ref_borrows_column_storage_until_materialization() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![SlotDescriptor {
                id: SlotId(0),
                name: "content".to_string(),
                slot_type: SlotType::logical(LogicalType::Text),
            }])
            .unwrap(),
        );
        let values: Arc<[String]> = vec!["borrowed content".to_string()].into();
        let source_ptr = values[0].as_ptr();
        let column = Arc::new(ColumnVector::Utf8 {
            values,
            validity: Validity::all(1),
        });
        let batch = ColumnarBatch::try_new(schema, vec![column]).unwrap();

        let value = batch.value_ref(SlotId(0), 0).unwrap();
        assert_eq!(value.logical_type(), Some(LogicalType::String));
        assert_eq!(value.as_str().unwrap().as_ptr(), source_ptr);
        assert_eq!(
            value.to_owned_value(),
            Value::String("borrowed content".into())
        );
    }

    #[test]
    fn logical_and_physical_slot_types_are_validated_separately() {
        assert!(SlotType::logical(LogicalType::Any).accepts(ColumnType::Bool));
        assert!(SlotType::logical(LogicalType::Any).accepts(ColumnType::Utf8));
        assert!(!SlotType::logical(LogicalType::Any).accepts(ColumnType::NodeId));
        assert!(SlotType::logical(LogicalType::String).accepts(ColumnType::Utf8));
        assert!(SlotType::logical(LogicalType::Text).accepts(ColumnType::Utf8));
        assert!(SlotType::NodeId.accepts(ColumnType::NodeId));
        assert!(!SlotType::NodeId.accepts(ColumnType::Int64));
        assert_eq!(SlotType::NodeId.logical_type(), Some(LogicalType::Int64));
        assert_eq!(SlotType::RelationalRowLocator.logical_type(), None);
    }

    #[test]
    fn relational_locator_projection_preserves_compact_identity_storage() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![SlotDescriptor {
                id: SlotId(0),
                name: "row_locator".to_string(),
                slot_type: SlotType::RelationalRowLocator,
            }])
            .unwrap(),
        );
        let locator = RelationalRowLocator::new(
            3,
            RelationalKey(vec![
                RelationalValue::BigInt(7),
                RelationalValue::Text("thread-1".to_string()),
            ]),
        );
        let nested_bytes = locator.allocated_bytes();
        let column = Arc::new(ColumnVector::relational_row_locators(vec![locator]));
        let batch = ColumnarBatch::try_new(schema, vec![Arc::clone(&column)]).unwrap();
        let projected = batch.project(&[SlotId(0)]).unwrap();

        assert!(Arc::ptr_eq(projected.column(SlotId(0)).unwrap(), &column));
        let actual = projected
            .column(SlotId(0))
            .unwrap()
            .relational_row_locator(0)
            .unwrap();
        assert_eq!(actual.table_id(), 3);
        assert_eq!(actual.primary_key().0.len(), 2);
        assert_eq!(column.value(0), None);
        assert_eq!(
            column.estimated_memory_bytes(),
            std::mem::size_of::<RelationalRowLocator>() + nested_bytes
        );
    }

    #[test]
    fn boolean_filter_limit_and_projection_share_storage() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![
                SlotDescriptor {
                    id: SlotId(0),
                    name: "visible".to_string(),
                    slot_type: SlotType::logical(LogicalType::Boolean),
                },
                SlotDescriptor {
                    id: SlotId(1),
                    name: "id".to_string(),
                    slot_type: SlotType::NodeId,
                },
            ])
            .unwrap(),
        );
        let visible = Arc::new(
            ColumnVector::boolean(
                vec![true, false, true, true, false],
                Validity::Bitmap {
                    len: 5,
                    words: Arc::from([0b1_1011]),
                },
            )
            .unwrap(),
        );
        let ids = Arc::new(ColumnVector::node_ids(vec![10, 11, 12, 13, 14]));
        let batch = ColumnarBatch::try_new(schema, vec![Arc::clone(&visible), Arc::clone(&ids)])
            .unwrap()
            .filter_boolean(SlotId(0), true)
            .unwrap()
            .limit(1, 1)
            .project(&[SlotId(1)])
            .unwrap();

        assert_eq!(batch.selection().iter().collect::<Vec<_>>(), vec![3]);
        assert!(Arc::ptr_eq(batch.column(SlotId(0)).unwrap(), &ids));
    }

    #[test]
    fn count_and_sum_follow_selection_and_validity() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![SlotDescriptor {
                id: SlotId(0),
                name: "token_count".to_string(),
                slot_type: SlotType::logical(LogicalType::Int64),
            }])
            .unwrap(),
        );
        let values = Arc::new(
            ColumnVector::int64(
                vec![4, 8, 16, 32],
                Validity::Bitmap {
                    len: 4,
                    words: Arc::from([0b1101]),
                },
            )
            .unwrap(),
        );
        let batch = ColumnarBatch::try_new(schema, vec![values])
            .unwrap()
            .filter_numeric(SlotId(0), ComparisonOp::Gte, NumericLiteral::Int(4))
            .unwrap()
            .limit(1, 2);

        assert_eq!(batch.count_selected(), 2);
        assert_eq!(batch.count_valid(SlotId(0)).unwrap(), 2);
        assert_eq!(batch.sum_int64(SlotId(0)).unwrap(), Some(48));
    }

    #[test]
    fn int64_sum_fails_closed_on_overflow() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![SlotDescriptor {
                id: SlotId(0),
                name: "value".to_string(),
                slot_type: SlotType::logical(LogicalType::Int64),
            }])
            .unwrap(),
        );
        let values = Arc::new(ColumnVector::int64(vec![i64::MAX, 1], Validity::all(2)).unwrap());
        let batch = ColumnarBatch::try_new(schema, vec![values]).unwrap();

        let error = batch.sum_int64(SlotId(0)).unwrap_err();
        assert!(error.to_string().contains("SUM overflow"));
    }
}
