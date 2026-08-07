use crate::{
    FileSegmentRangeReader, SegmentRangeReader, SegmentReadRange, SnapshotCommitError,
    SnapshotCoordinator, SnapshotReadGuard,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

mod codec;
mod overflow;

pub use codec::{
    decode_relational_checkpoint, decode_relational_checkpoint_file, decode_relational_wal_batch,
    encode_relational_checkpoint, encode_relational_checkpoint_to_writer,
    encode_relational_wal_batch, RelationalCheckpoint, RelationalDecodeLimits, RelationalWalBatch,
};
pub use overflow::{
    RelationalHydrationBudget, RelationalOverflowConfig, RelationalOverflowRef,
    DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES, DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
};

pub const DEFAULT_MAX_RELATIONAL_MUTATION_ROWS: usize = 100_000;
pub const DEFAULT_MAX_RELATIONAL_MUTATION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalMutationLimits {
    pub max_rows: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
}

impl Default for RelationalMutationLimits {
    fn default() -> Self {
        Self {
            max_rows: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_MUTATION_ROWS)
                .expect("default relational mutation row limit is non-zero"),
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_MUTATION_BYTES)
                .expect("default relational mutation byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelationalScalarType {
    Boolean,
    BigInt,
    DoublePrecision,
    Text,
    Bytea,
}

#[derive(Debug, Clone)]
pub enum RelationalValue {
    Null,
    Boolean(bool),
    BigInt(i64),
    DoublePrecision(f64),
    Text(String),
    Bytea(Vec<u8>),
    Overflow(RelationalOverflowRef),
}

impl RelationalValue {
    pub fn scalar_type(&self) -> Option<RelationalScalarType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(RelationalScalarType::Boolean),
            Self::BigInt(_) => Some(RelationalScalarType::BigInt),
            Self::DoublePrecision(_) => Some(RelationalScalarType::DoublePrecision),
            Self::Text(_) => Some(RelationalScalarType::Text),
            Self::Bytea(_) => Some(RelationalScalarType::Bytea),
            Self::Overflow(reference) => Some(reference.scalar_type),
        }
    }

    pub fn estimated_payload_bytes(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) | Self::DoublePrecision(_) => 8,
            Self::Text(value) => value.len(),
            Self::Bytea(value) => value.len(),
            Self::Overflow(_) => std::mem::size_of::<RelationalOverflowRef>(),
        }
    }

    fn kind_rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) => 2,
            Self::DoublePrecision(_) => 3,
            Self::Text(_) => 4,
            Self::Bytea(_) => 5,
            Self::Overflow(_) => 6,
        }
    }
}

impl PartialEq for RelationalValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RelationalValue {}

impl PartialOrd for RelationalValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelationalValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind_rank()
            .cmp(&other.kind_rank())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
                (Self::BigInt(left), Self::BigInt(right)) => left.cmp(right),
                (Self::DoublePrecision(left), Self::DoublePrecision(right)) => {
                    left.total_cmp(right)
                }
                (Self::Text(left), Self::Text(right)) => left.cmp(right),
                (Self::Bytea(left), Self::Bytea(right)) => left.cmp(right),
                (Self::Overflow(left), Self::Overflow(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

impl Hash for RelationalValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind_rank().hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::BigInt(value) => value.hash(state),
            Self::DoublePrecision(value) => value.to_bits().hash(state),
            Self::Text(value) => value.hash(state),
            Self::Bytea(value) => value.hash(state),
            Self::Overflow(reference) => reference.hash(state),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalColumnSchema {
    pub name: String,
    pub scalar_type: RelationalScalarType,
    pub nullable: bool,
    pub default: Option<RelationalValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalReferentialAction {
    NoAction,
    Restrict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalForeignKeySchema {
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: RelationalReferentialAction,
    pub on_update: RelationalReferentialAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalTableSchema {
    pub name: String,
    pub columns: Vec<RelationalColumnSchema>,
    pub primary_key: Vec<String>,
    pub unique_constraints: Vec<Vec<String>>,
    pub foreign_keys: Vec<RelationalForeignKeySchema>,
    pub indexes: Vec<RelationalIndexSchema>,
}

impl RelationalTableSchema {
    pub fn column_position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalKey(pub Vec<RelationalValue>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRow {
    values: Arc<[RelationalValue]>,
}

impl RelationalRow {
    pub fn new(values: Vec<RelationalValue>) -> Self {
        Self {
            values: Arc::from(values),
        }
    }

    pub fn values(&self) -> &[RelationalValue] {
        &self.values
    }

    fn estimated_payload_bytes(&self) -> usize {
        self.values
            .iter()
            .map(RelationalValue::estimated_payload_bytes)
            .sum()
    }
}

#[derive(Debug, Clone, Default)]
struct RelationalTableSegment {
    rows: RelationalRowPages,
    indexes: BTreeMap<String, RelationalIndexPages>,
}

const RELATIONAL_ROW_PAGE_MAX_ENTRIES: usize = 256;
const RELATIONAL_ROW_PAGE_TARGET_BYTES: usize = 1024 * 1024;

/// An ordered row map backed by immutable COW pages.
///
/// A relational snapshot shares every page. A mutation clones only the page
/// containing the affected primary key instead of cloning the entire table.
#[derive(Debug, Clone, Default)]
struct RelationalRowPages {
    pages: Arc<Vec<Arc<BTreeMap<RelationalKey, RelationalRow>>>>,
    len: usize,
}

impl RelationalRowPages {
    fn from_map(rows: BTreeMap<RelationalKey, RelationalRow>) -> Self {
        let len = rows.len();
        let mut pages = Vec::new();
        let mut page = BTreeMap::new();
        let mut page_bytes = 0usize;
        for (key, row) in rows {
            let entry_bytes = relational_row_entry_bytes(&key, &row);
            if !page.is_empty()
                && (page.len() >= RELATIONAL_ROW_PAGE_MAX_ENTRIES
                    || page_bytes.saturating_add(entry_bytes) > RELATIONAL_ROW_PAGE_TARGET_BYTES)
            {
                pages.push(Arc::new(std::mem::take(&mut page)));
                page_bytes = 0;
            }
            page_bytes = page_bytes.saturating_add(entry_bytes);
            page.insert(key, row);
        }
        if !page.is_empty() {
            pages.push(Arc::new(page));
        }
        Self {
            pages: Arc::new(pages),
            len,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self.pages.partition_point(|page| {
            page.last_key_value()
                .is_some_and(|(last_key, _)| last_key < key)
        });
        Some(index.min(self.pages.len() - 1))
    }

    fn get(&self, key: &RelationalKey) -> Option<&RelationalRow> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get(key))
    }

    fn get_key_value(&self, key: &RelationalKey) -> Option<(&RelationalKey, &RelationalRow)> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get_key_value(key))
    }

    fn contains_key(&self, key: &RelationalKey) -> bool {
        self.get(key).is_some()
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&RelationalKey, &RelationalRow)> {
        self.pages.iter().flat_map(|page| page.iter())
    }

    fn values(&self) -> impl DoubleEndedIterator<Item = &RelationalRow> {
        self.iter().map(|(_, row)| row)
    }

    fn insert(&mut self, key: RelationalKey, row: RelationalRow) -> Option<RelationalRow> {
        if self.pages.is_empty() {
            self.pages = Arc::new(vec![Arc::new(BTreeMap::from([(key, row)]))]);
            self.len = 1;
            return None;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty relational row pages have a target page");
        let pages = Arc::make_mut(&mut self.pages);
        let previous = Arc::make_mut(&mut pages[index]).insert(key, row);
        if previous.is_none() {
            self.len = self.len.saturating_add(1);
        }
        split_relational_row_page(pages, index);
        previous
    }

    fn remove(&mut self, key: &RelationalKey) -> Option<RelationalRow> {
        let index = self.page_index(key)?;
        if !self.pages[index].contains_key(key) {
            return None;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let removed = Arc::make_mut(&mut pages[index]).remove(key);
        if removed.is_some() {
            self.len = self.len.saturating_sub(1);
            if pages[index].is_empty() {
                pages.remove(index);
            }
        }
        removed
    }

    #[cfg(test)]
    fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    fn shared_page_count(&self, other: &Self) -> usize {
        self.pages
            .iter()
            .filter(|left| other.pages.iter().any(|right| Arc::ptr_eq(left, right)))
            .count()
    }
}

fn relational_row_entry_bytes(key: &RelationalKey, row: &RelationalRow) -> usize {
    std::mem::size_of::<RelationalKey>()
        .saturating_add(std::mem::size_of::<RelationalRow>())
        .saturating_add(
            key.0
                .iter()
                .map(RelationalValue::estimated_payload_bytes)
                .sum::<usize>(),
        )
        .saturating_add(row.estimated_payload_bytes())
}

fn split_relational_row_page(
    pages: &mut Vec<Arc<BTreeMap<RelationalKey, RelationalRow>>>,
    index: usize,
) {
    let page = Arc::make_mut(&mut pages[index]);
    let page_bytes = page
        .iter()
        .map(|(key, row)| relational_row_entry_bytes(key, row))
        .sum::<usize>();
    if page.len() <= 1
        || (page.len() <= RELATIONAL_ROW_PAGE_MAX_ENTRIES
            && page_bytes <= RELATIONAL_ROW_PAGE_TARGET_BYTES)
    {
        return;
    }
    let split_key = page
        .keys()
        .nth(page.len() / 2)
        .cloned()
        .expect("oversized relational row page is non-empty");
    let right = page.split_off(&split_key);
    pages.insert(index + 1, Arc::new(right));
}

const RELATIONAL_INDEX_PAGE_MAX_KEYS: usize = 256;
const RELATIONAL_POSTING_PAGE_MAX_KEYS: usize = 256;

#[derive(Debug, Clone, Default)]
struct RelationalKeySetPages {
    pages: Arc<Vec<Arc<BTreeSet<RelationalKey>>>>,
    len: usize,
}

impl RelationalKeySetPages {
    fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self
            .pages
            .partition_point(|page| page.last().is_some_and(|last_key| last_key < key));
        Some(index.min(self.pages.len() - 1))
    }

    fn insert(&mut self, key: RelationalKey) -> bool {
        if self.pages.is_empty() {
            self.pages = Arc::new(vec![Arc::new(BTreeSet::from([key]))]);
            self.len = 1;
            return true;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty posting pages have a target page");
        let pages = Arc::make_mut(&mut self.pages);
        let inserted = Arc::make_mut(&mut pages[index]).insert(key);
        if inserted {
            self.len = self.len.saturating_add(1);
        }
        if pages[index].len() > RELATIONAL_POSTING_PAGE_MAX_KEYS {
            let page = Arc::make_mut(&mut pages[index]);
            let split_key = page
                .iter()
                .nth(page.len() / 2)
                .cloned()
                .expect("oversized posting page is non-empty");
            let right = page.split_off(&split_key);
            pages.insert(index + 1, Arc::new(right));
        }
        inserted
    }

    fn remove(&mut self, key: &RelationalKey) -> bool {
        let Some(index) = self.page_index(key) else {
            return false;
        };
        if !self.pages[index].contains(key) {
            return false;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let removed = Arc::make_mut(&mut pages[index]).remove(key);
        if removed {
            self.len = self.len.saturating_sub(1);
            if pages[index].is_empty() {
                pages.remove(index);
            }
        }
        removed
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = &RelationalKey> {
        self.pages.iter().flat_map(|page| page.iter())
    }

    #[cfg(test)]
    fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    fn shared_page_count(&self, other: &Self) -> usize {
        self.pages
            .iter()
            .filter(|left| other.pages.iter().any(|right| Arc::ptr_eq(left, right)))
            .count()
    }
}

#[derive(Debug, Clone, Default)]
struct RelationalIndexPages {
    pages: Arc<Vec<Arc<BTreeMap<RelationalKey, RelationalKeySetPages>>>>,
}

impl RelationalIndexPages {
    fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self.pages.partition_point(|page| {
            page.last_key_value()
                .is_some_and(|(last_key, _)| last_key < key)
        });
        Some(index.min(self.pages.len() - 1))
    }

    fn get(&self, key: &RelationalKey) -> Option<&RelationalKeySetPages> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get(key))
    }

    fn insert_posting(&mut self, index_key: RelationalKey, primary_key: RelationalKey) -> bool {
        if self.pages.is_empty() {
            let mut postings = RelationalKeySetPages::default();
            postings.insert(primary_key);
            self.pages = Arc::new(vec![Arc::new(BTreeMap::from([(index_key, postings)]))]);
            return true;
        }
        let index = self
            .page_index(&index_key)
            .expect("non-empty index pages have a target page");
        let pages = Arc::make_mut(&mut self.pages);
        let page = Arc::make_mut(&mut pages[index]);
        let inserted = page.entry(index_key).or_default().insert(primary_key);
        if page.len() > RELATIONAL_INDEX_PAGE_MAX_KEYS {
            let split_key = page
                .keys()
                .nth(page.len() / 2)
                .cloned()
                .expect("oversized index page is non-empty");
            let right = page.split_off(&split_key);
            pages.insert(index + 1, Arc::new(right));
        }
        inserted
    }

    fn remove_posting(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) {
        let Some(index) = self.page_index(index_key) else {
            return;
        };
        if !self.pages[index].contains_key(index_key) {
            return;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let page = Arc::make_mut(&mut pages[index]);
        let remove_key = page
            .get_mut(index_key)
            .is_some_and(|postings| postings.remove(primary_key) && postings.is_empty());
        if remove_key {
            page.remove(index_key);
        }
        if page.is_empty() {
            pages.remove(index);
        }
    }

    fn prefix_cardinality(&self, prefix: &RelationalKey) -> usize {
        self.prefix_cardinality_at_most(prefix, usize::MAX)
    }

    fn prefix_cardinality_at_most(&self, prefix: &RelationalKey, max_count: usize) -> usize {
        if max_count == 0 {
            return 0;
        }
        let mut cardinality = 0usize;
        self.visit_prefix(prefix, |postings| {
            cardinality = cardinality.saturating_add(postings.len);
            cardinality < max_count
        });
        cardinality.min(max_count)
    }

    fn prefix_primary_keys<'a>(
        &'a self,
        prefix: &RelationalKey,
        max_keys: usize,
    ) -> Vec<&'a RelationalKey> {
        if max_keys == 0 {
            return Vec::new();
        }
        let mut keys = Vec::new();
        self.visit_prefix(prefix, |postings| {
            for key in postings.iter() {
                keys.push(key);
                if keys.len() >= max_keys {
                    return false;
                }
            }
            true
        });
        keys
    }

    fn visit_prefix<'a>(
        &'a self,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'a RelationalKeySetPages) -> bool,
    ) {
        let Some(start) = self.page_index(prefix) else {
            return;
        };
        'pages: for page in &self.pages[start..] {
            for (key, postings) in page.range(prefix.clone()..) {
                if !relational_key_has_prefix(key, prefix) {
                    break 'pages;
                }
                if !visit(postings) {
                    break 'pages;
                }
            }
        }
    }
}

fn relational_key_has_prefix(key: &RelationalKey, prefix: &RelationalKey) -> bool {
    key.0.len() >= prefix.0.len() && key.0[..prefix.0.len()] == prefix.0
}

#[derive(Debug, Clone, Copy)]
pub struct RelationalIndexPosting<'a> {
    postings: &'a RelationalKeySetPages,
}

impl<'a> RelationalIndexPosting<'a> {
    pub fn len(self) -> usize {
        self.postings.len
    }

    pub fn is_empty(self) -> bool {
        self.postings.is_empty()
    }

    pub fn contains(self, key: &RelationalKey) -> bool {
        self.postings
            .page_index(key)
            .is_some_and(|index| self.postings.pages[index].contains(key))
    }

    pub fn iter(self) -> impl DoubleEndedIterator<Item = &'a RelationalKey> {
        self.postings.iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RelationalState {
    schemas: BTreeMap<String, Arc<RelationalTableSchema>>,
    segments: BTreeMap<String, Arc<RelationalTableSegment>>,
    overflow_segments: BTreeMap<String, RelationalOverflowSegment>,
}

#[derive(Debug, Clone)]
enum RelationalOverflowSegment {
    Inline(Arc<[u8]>),
    FileRange {
        reader: Arc<FileSegmentRangeReader>,
        range: SegmentReadRange,
    },
}

impl RelationalOverflowSegment {
    fn read(&self) -> Result<Arc<[u8]>, RelationalError> {
        match self {
            Self::Inline(bytes) => Ok(Arc::clone(bytes)),
            Self::FileRange { reader, range } => reader.read_range(range).map_err(|error| {
                RelationalError::Corruption(format!(
                    "failed to read file-backed overflow segment: {error}"
                ))
            }),
        }
    }

    fn is_file_backed(&self) -> bool {
        matches!(self, Self::FileRange { .. })
    }
}

impl RelationalState {
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    pub fn stage_transaction(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<Self, RelationalError> {
        admit_transaction(&transaction, limits)?;
        apply_transaction(self, transaction, limits, overflow_config)
    }

    pub fn table_schema(&self, table: &str) -> Option<&RelationalTableSchema> {
        self.schemas.get(table).map(Arc::as_ref)
    }

    pub fn row(&self, table: &str, key: &RelationalKey) -> Option<&RelationalRow> {
        self.segments.get(table)?.rows.get(key)
    }

    pub fn row_entry(
        &self,
        table: &str,
        key: &RelationalKey,
    ) -> Option<(&RelationalKey, &RelationalRow)> {
        self.segments.get(table)?.rows.get_key_value(key)
    }

    pub fn rows(
        &self,
        table: &str,
    ) -> impl DoubleEndedIterator<Item = (&RelationalKey, &RelationalRow)> {
        self.segments
            .get(table)
            .into_iter()
            .flat_map(|segment| segment.rows.iter())
    }

    pub fn row_count(&self, table: &str) -> usize {
        self.segments
            .get(table)
            .map_or(0, |segment| segment.rows.len())
    }

    pub fn index_lookup(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
    ) -> Option<RelationalIndexPosting<'_>> {
        self.segments
            .get(table)?
            .indexes
            .get(index)?
            .get(key)
            .map(|postings| RelationalIndexPosting { postings })
    }

    pub fn index_prefix_cardinality(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
    ) -> Option<usize> {
        Some(
            self.segments
                .get(table)?
                .indexes
                .get(index)?
                .prefix_cardinality(prefix),
        )
    }

    pub fn index_prefix_cardinality_at_most(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        max_count: usize,
    ) -> Option<usize> {
        Some(
            self.segments
                .get(table)?
                .indexes
                .get(index)?
                .prefix_cardinality_at_most(prefix, max_count),
        )
    }

    pub fn index_prefix_lookup<'a>(
        &'a self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        max_keys: usize,
    ) -> Option<Vec<&'a RelationalKey>> {
        Some(
            self.segments
                .get(table)?
                .indexes
                .get(index)?
                .prefix_primary_keys(prefix, max_keys),
        )
    }

    /// Visits rows selected by a leading index-key prefix without first
    /// materializing the complete posting list.
    ///
    /// Returning `false` from `visit` stops the scan. `None` means that the
    /// table or materialized index does not exist.
    pub fn visit_index_prefix_rows<'a>(
        &'a self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'a RelationalKey, &'a RelationalRow) -> bool,
    ) -> Option<()> {
        let segment = self.segments.get(table)?;
        let index = segment.indexes.get(index)?;
        index.visit_prefix(prefix, |postings| {
            for primary_key in postings.iter() {
                let row = segment
                    .rows
                    .get(primary_key)
                    .expect("materialized relational index points to an existing row");
                if !visit(primary_key, row) {
                    return false;
                }
            }
            true
        });
        Some(())
    }

    pub fn hydrate_row(
        &self,
        table: &str,
        key: &RelationalKey,
        budget: &mut RelationalHydrationBudget,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        self.hydrate_row_with_context(table, key, budget, None)
    }

    pub fn hydrate_row_with_context(
        &self,
        table: &str,
        key: &RelationalKey,
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        let Some(row) = self.row(table, key) else {
            return Ok(None);
        };
        overflow::hydrate_row(self, row, budget, task_context).map(Some)
    }

    pub fn overflow_segment_count(&self) -> usize {
        self.overflow_segments.len()
    }

    pub fn file_backed_overflow_segment_count(&self) -> usize {
        self.overflow_segments
            .values()
            .filter(|segment| segment.is_file_backed())
            .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalInsertMode {
    Error,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalComparisonOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalPredicate {
    And(Box<RelationalPredicate>, Box<RelationalPredicate>),
    Or(Box<RelationalPredicate>, Box<RelationalPredicate>),
    Not(Box<RelationalPredicate>),
    Compare {
        column: String,
        op: RelationalComparisonOp,
        value: RelationalValue,
    },
    IsNull {
        column: String,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalUpsertValue {
    ExcludedColumn(String),
    Value(RelationalValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalUpsertAssignment {
    pub column: String,
    pub value: RelationalUpsertValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalUpdateValue {
    Column(String),
    Value(RelationalValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalUpdateAssignment {
    pub column: String,
    pub value: RelationalUpdateValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalConflictAction {
    DoNothing,
    Update(Vec<RelationalUpsertAssignment>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalWrite {
    CreateTable(RelationalTableSchema),
    CreateIndex {
        table: String,
        index: RelationalIndexSchema,
    },
    Insert {
        table: String,
        rows: Vec<RelationalRow>,
        mode: RelationalInsertMode,
    },
    Upsert {
        table: String,
        rows: Vec<RelationalRow>,
        conflict_columns: Vec<String>,
        action: RelationalConflictAction,
    },
    DeleteByPrimaryKey {
        table: String,
        keys: Vec<RelationalKey>,
    },
    DeleteWhere {
        table: String,
        predicate: RelationalPredicate,
    },
    UpdateWhere {
        table: String,
        assignments: Vec<RelationalUpdateAssignment>,
        predicate: RelationalPredicate,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalTransaction {
    pub writes: Vec<RelationalWrite>,
}

impl RelationalTransaction {
    pub fn estimated_mutation_rows(&self) -> usize {
        self.writes
            .iter()
            .map(|write| match write {
                RelationalWrite::Insert { rows, .. } => rows.len(),
                RelationalWrite::Upsert { rows, .. } => rows.len(),
                RelationalWrite::DeleteByPrimaryKey { keys, .. } => keys.len(),
                RelationalWrite::CreateTable(_)
                | RelationalWrite::CreateIndex { .. }
                | RelationalWrite::DeleteWhere { .. }
                | RelationalWrite::UpdateWhere { .. } => 0,
            })
            .sum()
    }

    pub fn estimated_payload_bytes(&self) -> usize {
        self.writes
            .iter()
            .map(|write| match write {
                RelationalWrite::Insert { rows, .. } => rows
                    .iter()
                    .map(RelationalRow::estimated_payload_bytes)
                    .sum(),
                RelationalWrite::Upsert { rows, .. } => rows
                    .iter()
                    .map(RelationalRow::estimated_payload_bytes)
                    .sum(),
                RelationalWrite::DeleteByPrimaryKey { keys, .. } => keys
                    .iter()
                    .flat_map(|key| key.0.iter())
                    .map(RelationalValue::estimated_payload_bytes)
                    .sum(),
                RelationalWrite::CreateTable(_)
                | RelationalWrite::CreateIndex { .. }
                | RelationalWrite::DeleteWhere { .. }
                | RelationalWrite::UpdateWhere { .. } => 0,
            })
            .sum()
    }
}

#[derive(Debug)]
pub struct RelationalStore {
    snapshots: SnapshotCoordinator<RelationalState>,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
}

impl RelationalStore {
    pub fn new(limits: RelationalMutationLimits) -> Self {
        Self::with_overflow_config(limits, RelationalOverflowConfig::default())
    }

    pub fn with_overflow_config(
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Self {
        Self {
            snapshots: SnapshotCoordinator::new(RelationalState::default()),
            limits,
            overflow_config,
        }
    }

    pub fn snapshot(&self) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<()>> {
        self.snapshots.read()
    }

    pub fn commit(
        &self,
        transaction: RelationalTransaction,
        make_durable: impl FnOnce(u64, &RelationalState) -> Result<(), RelationalError>,
    ) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<RelationalError>> {
        self.admit(&transaction)
            .map_err(SnapshotCommitError::Stage)?;
        let overflow_config = self.overflow_config;
        let limits = self.limits;
        self.snapshots.commit(
            move |state, _| apply_transaction(state, transaction, limits, overflow_config),
            make_durable,
        )
    }

    pub fn commit_with_wal(
        &self,
        transaction: RelationalTransaction,
        persist: impl FnOnce(u64, &[u8]) -> Result<(), RelationalError>,
    ) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<RelationalError>> {
        let durable_transaction = transaction.clone();
        self.commit(transaction, move |epoch, _| {
            let record = encode_relational_wal_batch(epoch, &durable_transaction)?;
            let max_record_bytes = RelationalDecodeLimits::wal().max_record_bytes;
            if record.len() > max_record_bytes {
                return Err(RelationalError::Admission(format!(
                    "relational WAL record contains {} bytes, exceeding max_record_bytes {max_record_bytes}",
                    record.len()
                )));
            }
            persist(epoch, &record)
        })
    }

    pub fn replay_wal_record(
        &self,
        record: &[u8],
        decode_limits: RelationalDecodeLimits,
    ) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<RelationalError>> {
        let batch = decode_relational_wal_batch(record, decode_limits)
            .map_err(SnapshotCommitError::Stage)?;
        self.admit(&batch.transaction)
            .map_err(SnapshotCommitError::Stage)?;
        let expected_epoch = batch.epoch;
        let transaction = batch.transaction;
        let overflow_config = self.overflow_config;
        let limits = self.limits;
        self.snapshots.commit(
            move |state, next_epoch| {
                if next_epoch != expected_epoch {
                    return Err(RelationalError::Corruption(format!(
                        "relational WAL epoch gap: expected {next_epoch}, found {expected_epoch}"
                    )));
                }
                apply_transaction(state, transaction, limits, overflow_config)
            },
            |_, _| Ok(()),
        )
    }

    pub fn encode_checkpoint(&self) -> Result<Vec<u8>, RelationalError> {
        let snapshot = self.snapshots.read().map_err(|error| {
            RelationalError::Durability(format!("failed to pin relational snapshot: {error:?}"))
        })?;
        encode_relational_checkpoint(snapshot.epoch(), snapshot.value())
    }

    pub fn from_checkpoint(
        checkpoint: &[u8],
        decode_limits: RelationalDecodeLimits,
        mutation_limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<Self, RelationalError> {
        let checkpoint = decode_relational_checkpoint(checkpoint, decode_limits)?;
        Ok(Self {
            snapshots: SnapshotCoordinator::new_at_epoch(checkpoint.state, checkpoint.epoch),
            limits: mutation_limits,
            overflow_config,
        })
    }

    fn admit(&self, transaction: &RelationalTransaction) -> Result<(), RelationalError> {
        admit_transaction(transaction, self.limits)
    }
}

impl Default for RelationalStore {
    fn default() -> Self {
        Self::new(RelationalMutationLimits::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalError {
    Admission(String),
    Schema(String),
    Constraint(String),
    Durability(String),
    Corruption(String),
}

impl fmt::Display for RelationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => write!(formatter, "relational admission failed: {message}"),
            Self::Schema(message) => write!(formatter, "relational schema error: {message}"),
            Self::Constraint(message) => {
                write!(formatter, "relational constraint violation: {message}")
            }
            Self::Durability(message) => {
                write!(formatter, "relational durability error: {message}")
            }
            Self::Corruption(message) => {
                write!(formatter, "relational corruption: {message}")
            }
        }
    }
}

impl std::error::Error for RelationalError {}

fn admit_transaction(
    transaction: &RelationalTransaction,
    limits: RelationalMutationLimits,
) -> Result<(), RelationalError> {
    let rows = transaction.estimated_mutation_rows();
    if rows > limits.max_rows.get() {
        return Err(RelationalError::Admission(format!(
            "relational mutation contains {rows} rows, exceeding max_rows {}",
            limits.max_rows
        )));
    }
    let bytes = transaction.estimated_payload_bytes();
    if bytes > limits.max_payload_bytes.get() {
        return Err(RelationalError::Admission(format!(
            "relational mutation contains {bytes} payload bytes, exceeding max_payload_bytes {}",
            limits.max_payload_bytes
        )));
    }
    Ok(())
}

fn apply_transaction(
    state: &RelationalState,
    transaction: RelationalTransaction,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
) -> Result<RelationalState, RelationalError> {
    let mut next = state.clone();
    let mut touched = BTreeSet::new();
    let mut changed_keys = BTreeMap::<String, BTreeSet<RelationalKey>>::new();
    let mut full_index_rebuild = BTreeSet::new();
    for write in transaction.writes {
        match write {
            RelationalWrite::CreateTable(schema) => {
                validate_table_schema(&schema)?;
                if next.schemas.contains_key(&schema.name) {
                    return Err(RelationalError::Schema(format!(
                        "table {} already exists",
                        schema.name
                    )));
                }
                touched.insert(schema.name.clone());
                full_index_rebuild.insert(schema.name.clone());
                next.segments.insert(
                    schema.name.clone(),
                    Arc::new(RelationalTableSegment::default()),
                );
                next.schemas.insert(schema.name.clone(), Arc::new(schema));
            }
            RelationalWrite::CreateIndex { table, index } => {
                let schema = next
                    .schemas
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let schema = Arc::make_mut(schema);
                validate_column_list(schema, &index.columns, "index")?;
                if index.columns.is_empty()
                    || schema.indexes.iter().any(|item| item.name == index.name)
                {
                    return Err(RelationalError::Schema(format!(
                        "invalid or duplicate index {}",
                        index.name
                    )));
                }
                schema.indexes.push(index);
                full_index_rebuild.insert(table.clone());
                touched.insert(table);
            }
            RelationalWrite::Insert { table, rows, mode } => {
                let schema =
                    Arc::clone(next.schemas.get(&table).ok_or_else(|| {
                        RelationalError::Schema(format!("unknown table {table}"))
                    })?);
                let primary_key = column_positions(&schema, &schema.primary_key)?;
                let mut prepared_rows = Vec::with_capacity(rows.len());
                for mut row in rows {
                    validate_row(&schema, &row)?;
                    overflow::externalize_row(&mut next, &schema, &mut row, overflow_config)?;
                    prepared_rows.push(row);
                }
                let segment = next.segments.get_mut(&table).ok_or_else(|| {
                    RelationalError::Schema(format!("missing row segment for table {table}"))
                })?;
                let segment = Arc::make_mut(segment);
                for row in prepared_rows {
                    let key = row_key(&row, &primary_key);
                    if mode == RelationalInsertMode::Error && segment.rows.contains_key(&key) {
                        return Err(RelationalError::Constraint(format!(
                            "duplicate primary key in table {table}"
                        )));
                    }
                    segment.rows.insert(key.clone(), row);
                    changed_keys.entry(table.clone()).or_default().insert(key);
                }
                touched.insert(table);
            }
            RelationalWrite::Upsert {
                table,
                rows,
                conflict_columns,
                action,
            } => {
                apply_upsert(
                    &mut next,
                    &table,
                    rows,
                    &conflict_columns,
                    &action,
                    overflow_config,
                    changed_keys.entry(table.clone()).or_default(),
                )?;
                touched.insert(table);
            }
            RelationalWrite::DeleteByPrimaryKey { table, keys } => {
                let segment = next
                    .segments
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let segment = Arc::make_mut(segment);
                for key in keys {
                    changed_keys
                        .entry(table.clone())
                        .or_default()
                        .insert(key.clone());
                    segment.rows.remove(&key);
                }
                touched.insert(table);
            }
            RelationalWrite::DeleteWhere { table, predicate } => {
                let schema = next
                    .schemas
                    .get(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                validate_predicate(schema, &predicate)?;
                let segment = next
                    .segments
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let segment = Arc::make_mut(segment);
                let mut keys = Vec::new();
                for (key, row) in segment.rows.iter() {
                    if predicate_truth(schema, row, &predicate)? == Some(true) {
                        keys.push(key.clone());
                        if keys.len() > limits.max_rows.get() {
                            break;
                        }
                    }
                }
                if keys.len() > limits.max_rows.get() {
                    return Err(RelationalError::Admission(format!(
                        "relational DELETE affects more than max_rows {}",
                        limits.max_rows
                    )));
                }
                for key in keys {
                    changed_keys
                        .entry(table.clone())
                        .or_default()
                        .insert(key.clone());
                    segment.rows.remove(&key);
                }
                touched.insert(table);
            }
            RelationalWrite::UpdateWhere {
                table,
                assignments,
                predicate,
            } => {
                apply_update(
                    &mut next,
                    &table,
                    &assignments,
                    &predicate,
                    limits,
                    overflow_config,
                    changed_keys.entry(table.clone()).or_default(),
                )?;
                touched.insert(table);
            }
        }
    }
    overflow::prune_unreachable_segments(&mut next);
    for table in &touched {
        if full_index_rebuild.contains(table) {
            rebuild_indexes(&mut next, table)?;
        } else if let Some(keys) = changed_keys.get(table) {
            refresh_indexes_for_keys(state, &mut next, table, keys)?;
        }
    }
    validate_foreign_keys_incremental(state, &next, &changed_keys, &full_index_rebuild)?;
    Ok(next)
}

fn apply_upsert(
    state: &mut RelationalState,
    table: &str,
    rows: Vec<RelationalRow>,
    conflict_columns: &[String],
    action: &RelationalConflictAction,
    overflow_config: RelationalOverflowConfig,
    changed_keys: &mut BTreeSet<RelationalKey>,
) -> Result<(), RelationalError> {
    let schema = Arc::clone(
        state
            .schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    let conflict_positions = column_positions(&schema, conflict_columns)?;
    let conflict_is_unique = conflict_columns == schema.primary_key
        || schema
            .unique_constraints
            .iter()
            .any(|columns| columns == conflict_columns)
        || schema
            .indexes
            .iter()
            .any(|index| index.unique && index.columns == conflict_columns);
    if conflict_columns.is_empty() || !conflict_is_unique {
        return Err(RelationalError::Schema(format!(
            "UPSERT conflict target on table {table} must name a primary or unique key"
        )));
    }
    let primary_key_positions = column_positions(&schema, &schema.primary_key)?;
    let assignments = match action {
        RelationalConflictAction::DoNothing => Vec::new(),
        RelationalConflictAction::Update(assignments) => assignments
            .iter()
            .map(|assignment| {
                let target = schema.column_position(&assignment.column).ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "UPSERT assignment references unknown column {}",
                        assignment.column
                    ))
                })?;
                let source = match &assignment.value {
                    RelationalUpsertValue::ExcludedColumn(column) => {
                        let position = schema.column_position(column).ok_or_else(|| {
                            RelationalError::Schema(format!(
                                "UPSERT EXCLUDED references unknown column {column}"
                            ))
                        })?;
                        Some(position)
                    }
                    RelationalUpsertValue::Value(value) => {
                        validate_value_type(&schema.columns[target], value)?;
                        None
                    }
                };
                Ok((target, source, &assignment.value))
            })
            .collect::<Result<Vec<_>, RelationalError>>()?,
    };
    let mut staged_conflicts = BTreeMap::<RelationalKey, Option<RelationalKey>>::new();

    for mut excluded in rows {
        validate_row(&schema, &excluded)?;
        overflow::externalize_row(state, &schema, &mut excluded, overflow_config)?;
        let conflict_key = row_key(&excluded, &conflict_positions);
        let conflict_has_null = conflict_key
            .0
            .iter()
            .any(|value| matches!(value, RelationalValue::Null));
        let existing_primary_key = if conflict_has_null {
            None
        } else if let Some(staged) = staged_conflicts.get(&conflict_key) {
            staged.clone()
        } else {
            conflict_primary_key(state, table, &schema, conflict_columns, &conflict_key)?
        };

        let segment = state
            .segments
            .get_mut(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
        let segment = Arc::make_mut(segment);
        if let Some(existing_primary_key) = existing_primary_key {
            if matches!(action, RelationalConflictAction::DoNothing) {
                continue;
            }
            let existing = segment
                .rows
                .get(&existing_primary_key)
                .expect("conflict row was found in the same segment");
            let mut values = existing.values().to_vec();
            for (target, source, assignment) in &assignments {
                values[*target] = match (source, *assignment) {
                    (Some(source), _) => excluded.values()[*source].clone(),
                    (None, RelationalUpsertValue::Value(value)) => value.clone(),
                    (None, RelationalUpsertValue::ExcludedColumn(_)) => {
                        unreachable!("excluded source position was resolved")
                    }
                };
            }
            let updated = RelationalRow::new(values);
            validate_row(&schema, &updated)?;
            let updated_primary_key = row_key(&updated, &primary_key_positions);
            let updated_conflict_key = row_key(&updated, &conflict_positions);
            changed_keys.insert(existing_primary_key.clone());
            changed_keys.insert(updated_primary_key.clone());
            segment.rows.remove(&existing_primary_key);
            segment.rows.insert(updated_primary_key.clone(), updated);
            staged_conflicts.insert(conflict_key, None);
            if !key_contains_null(&updated_conflict_key) {
                staged_conflicts.insert(updated_conflict_key, Some(updated_primary_key));
            }
        } else {
            let primary_key = row_key(&excluded, &primary_key_positions);
            changed_keys.insert(primary_key.clone());
            if segment.rows.insert(primary_key.clone(), excluded).is_some() {
                return Err(RelationalError::Constraint(format!(
                    "duplicate primary key in table {table}"
                )));
            }
            if !conflict_has_null {
                staged_conflicts.insert(conflict_key, Some(primary_key));
            }
        }
    }
    Ok(())
}

fn conflict_primary_key(
    state: &RelationalState,
    table: &str,
    schema: &RelationalTableSchema,
    conflict_columns: &[String],
    conflict_key: &RelationalKey,
) -> Result<Option<RelationalKey>, RelationalError> {
    if conflict_columns == schema.primary_key {
        return Ok(state.row(table, conflict_key).map(|_| conflict_key.clone()));
    }
    let index_name = if let Some(ordinal) = schema
        .unique_constraints
        .iter()
        .position(|columns| columns == conflict_columns)
    {
        format!("__unique_{ordinal}")
    } else if let Some(index) = schema
        .indexes
        .iter()
        .find(|index| index.unique && index.columns == conflict_columns)
    {
        index.name.clone()
    } else {
        return Err(RelationalError::Schema(format!(
            "UPSERT conflict target on table {table} is not materialized"
        )));
    };
    let Some(postings) = state.index_lookup(table, &index_name, conflict_key) else {
        return Ok(None);
    };
    let mut keys = postings.iter();
    let primary_key = keys.next().cloned();
    if keys.next().is_some() {
        return Err(RelationalError::Corruption(format!(
            "unique conflict index {index_name} on table {table} has multiple visible rows"
        )));
    }
    Ok(primary_key)
}

fn apply_update(
    state: &mut RelationalState,
    table: &str,
    assignments: &[RelationalUpdateAssignment],
    predicate: &RelationalPredicate,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
    changed_keys: &mut BTreeSet<RelationalKey>,
) -> Result<(), RelationalError> {
    let schema = Arc::clone(
        state
            .schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    if assignments.is_empty() {
        return Err(RelationalError::Schema(
            "UPDATE requires at least one assignment".to_string(),
        ));
    }
    validate_predicate(&schema, predicate)?;
    let resolved = assignments
        .iter()
        .map(|assignment| {
            let target = schema.column_position(&assignment.column).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "UPDATE assignment references unknown column {}",
                    assignment.column
                ))
            })?;
            let source = match &assignment.value {
                RelationalUpdateValue::Column(column) => {
                    let source = schema.column_position(column).ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "UPDATE assignment references unknown source column {column}"
                        ))
                    })?;
                    if schema.columns[target].scalar_type != schema.columns[source].scalar_type {
                        return Err(RelationalError::Schema(format!(
                            "UPDATE assignment from {column} has an incompatible type"
                        )));
                    }
                    Some(source)
                }
                RelationalUpdateValue::Value(value) => {
                    validate_value_type(&schema.columns[target], value)?;
                    None
                }
            };
            Ok((target, source, &assignment.value))
        })
        .collect::<Result<Vec<_>, RelationalError>>()?;
    let segment = state
        .segments
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let mut matched = Vec::new();
    for (key, row) in segment.rows.iter() {
        if predicate_truth(&schema, row, predicate)? == Some(true) {
            matched.push((key.clone(), row.clone()));
            if matched.len() > limits.max_rows.get() {
                return Err(RelationalError::Admission(format!(
                    "relational UPDATE affects more than max_rows {}",
                    limits.max_rows
                )));
            }
        }
    }
    let primary_key_positions = column_positions(&schema, &schema.primary_key)?;
    let mut updates = Vec::with_capacity(matched.len());
    for (old_key, row) in &matched {
        let mut values = row.values().to_vec();
        for (target, source, assignment) in &resolved {
            values[*target] = match (source, *assignment) {
                (Some(source), _) => row.values()[*source].clone(),
                (None, RelationalUpdateValue::Value(value)) => value.clone(),
                (None, RelationalUpdateValue::Column(_)) => {
                    unreachable!("update source position was resolved")
                }
            };
        }
        let mut updated = RelationalRow::new(values);
        validate_row(&schema, &updated)?;
        overflow::externalize_row(state, &schema, &mut updated, overflow_config)?;
        let new_key = row_key(&updated, &primary_key_positions);
        updates.push((old_key.clone(), new_key, updated));
    }
    let segment = state
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let segment = Arc::make_mut(segment);
    for (old_key, _, _) in &updates {
        changed_keys.insert(old_key.clone());
        segment.rows.remove(old_key);
    }
    for (_, new_key, row) in updates {
        changed_keys.insert(new_key.clone());
        if segment.rows.insert(new_key, row).is_some() {
            return Err(RelationalError::Constraint(format!(
                "UPDATE creates a duplicate primary key in table {table}"
            )));
        }
    }
    Ok(())
}

fn validate_predicate(
    schema: &RelationalTableSchema,
    predicate: &RelationalPredicate,
) -> Result<(), RelationalError> {
    match predicate {
        RelationalPredicate::And(left, right) | RelationalPredicate::Or(left, right) => {
            validate_predicate(schema, left)?;
            validate_predicate(schema, right)
        }
        RelationalPredicate::Not(predicate) => validate_predicate(schema, predicate),
        RelationalPredicate::Compare { column, value, .. } => {
            let position = schema.column_position(column).ok_or_else(|| {
                RelationalError::Schema(format!("predicate references unknown column {column}"))
            })?;
            if !matches!(value, RelationalValue::Null)
                && value.scalar_type() != Some(schema.columns[position].scalar_type)
            {
                return Err(RelationalError::Schema(format!(
                    "predicate on column {column} has an incompatible value type"
                )));
            }
            Ok(())
        }
        RelationalPredicate::IsNull { column, .. } => {
            if schema.column_position(column).is_none() {
                return Err(RelationalError::Schema(format!(
                    "predicate references unknown column {column}"
                )));
            }
            Ok(())
        }
    }
}

fn predicate_truth(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
    predicate: &RelationalPredicate,
) -> Result<Option<bool>, RelationalError> {
    match predicate {
        RelationalPredicate::And(left, right) => match (
            predicate_truth(schema, row, left)?,
            predicate_truth(schema, row, right)?,
        ) {
            (Some(false), _) | (_, Some(false)) => Ok(Some(false)),
            (Some(true), Some(true)) => Ok(Some(true)),
            _ => Ok(None),
        },
        RelationalPredicate::Or(left, right) => match (
            predicate_truth(schema, row, left)?,
            predicate_truth(schema, row, right)?,
        ) {
            (Some(true), _) | (_, Some(true)) => Ok(Some(true)),
            (Some(false), Some(false)) => Ok(Some(false)),
            _ => Ok(None),
        },
        RelationalPredicate::Not(predicate) => {
            Ok(predicate_truth(schema, row, predicate)?.map(|value| !value))
        }
        RelationalPredicate::Compare { column, op, value } => {
            let position = schema
                .column_position(column)
                .expect("predicate schema was validated");
            let current = &row.values()[position];
            if matches!(current, RelationalValue::Overflow(_))
                || matches!(value, RelationalValue::Overflow(_))
            {
                return Err(RelationalError::Admission(format!(
                    "DELETE predicate on column {column} requires overflow hydration"
                )));
            }
            if matches!(current, RelationalValue::Null) || matches!(value, RelationalValue::Null) {
                return Ok(None);
            }
            Ok(Some(match op {
                RelationalComparisonOp::Eq => current == value,
                RelationalComparisonOp::NotEq => current != value,
                RelationalComparisonOp::Lt => current < value,
                RelationalComparisonOp::Lte => current <= value,
                RelationalComparisonOp::Gt => current > value,
                RelationalComparisonOp::Gte => current >= value,
            }))
        }
        RelationalPredicate::IsNull { column, negated } => {
            let position = schema
                .column_position(column)
                .expect("predicate schema was validated");
            Ok(Some(
                matches!(row.values()[position], RelationalValue::Null) != *negated,
            ))
        }
    }
}

fn validate_table_schema(schema: &RelationalTableSchema) -> Result<(), RelationalError> {
    if schema.name.is_empty() || schema.columns.is_empty() {
        return Err(RelationalError::Schema(
            "table name and columns must be non-empty".to_string(),
        ));
    }
    if schema.primary_key.is_empty() {
        return Err(RelationalError::Schema(format!(
            "table {} must declare a primary key",
            schema.name
        )));
    }
    let mut names = BTreeSet::new();
    for column in &schema.columns {
        if column.name.is_empty() || !names.insert(column.name.clone()) {
            return Err(RelationalError::Schema(format!(
                "duplicate or empty column {}",
                column.name
            )));
        }
        if let Some(default) = &column.default {
            validate_value_type(column, default)?;
        }
    }
    validate_column_list(schema, &schema.primary_key, "primary key")?;
    for column in &schema.primary_key {
        let position = schema.column_position(column).expect("validated column");
        if schema.columns[position].nullable {
            return Err(RelationalError::Schema(format!(
                "primary-key column {column} must be NOT NULL"
            )));
        }
    }
    for unique in &schema.unique_constraints {
        validate_column_list(schema, unique, "unique constraint")?;
    }
    for foreign_key in &schema.foreign_keys {
        validate_column_list(schema, &foreign_key.columns, "foreign key")?;
        if foreign_key.columns.len() != foreign_key.referenced_columns.len()
            || foreign_key.columns.is_empty()
        {
            return Err(RelationalError::Schema(
                "foreign-key column counts must match and be non-empty".to_string(),
            ));
        }
    }
    for index in &schema.indexes {
        validate_column_list(schema, &index.columns, "index")?;
    }
    Ok(())
}

fn validate_column_list(
    schema: &RelationalTableSchema,
    columns: &[String],
    kind: &str,
) -> Result<(), RelationalError> {
    let mut unique = BTreeSet::new();
    for column in columns {
        if schema.column_position(column).is_none() || !unique.insert(column) {
            return Err(RelationalError::Schema(format!(
                "{kind} references unknown or duplicate column {column}"
            )));
        }
    }
    Ok(())
}

fn validate_row(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
) -> Result<(), RelationalError> {
    if row.values.len() != schema.columns.len() {
        return Err(RelationalError::Schema(format!(
            "table {} expects {} columns but row has {}",
            schema.name,
            schema.columns.len(),
            row.values.len()
        )));
    }
    for (column, value) in schema.columns.iter().zip(row.values.iter()) {
        validate_value_type(column, value)?;
    }
    Ok(())
}

fn validate_value_type(
    column: &RelationalColumnSchema,
    value: &RelationalValue,
) -> Result<(), RelationalError> {
    if matches!(value, RelationalValue::Null) {
        if column.nullable {
            return Ok(());
        }
        return Err(RelationalError::Constraint(format!(
            "column {} is NOT NULL",
            column.name
        )));
    }
    if value.scalar_type() != Some(column.scalar_type) {
        return Err(RelationalError::Schema(format!(
            "column {} expects {:?} but received {:?}",
            column.name,
            column.scalar_type,
            value.scalar_type()
        )));
    }
    Ok(())
}

fn rebuild_indexes(state: &mut RelationalState, table: &str) -> Result<(), RelationalError> {
    let schema = state
        .schemas
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let mut index_schemas = schema.indexes.clone();
    for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
        index_schemas.push(RelationalIndexSchema {
            name: format!("__unique_{ordinal}"),
            columns: columns.clone(),
            unique: true,
        });
    }
    let segment = state
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let segment = Arc::make_mut(segment);
    segment.indexes.clear();
    for index in index_schemas {
        let positions = column_positions(schema, &index.columns)?;
        let mut postings = RelationalIndexPages::default();
        for (primary_key, row) in segment.rows.iter() {
            let key = row_key(row, &positions);
            if index.unique
                && key
                    .0
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Null))
            {
                continue;
            }
            if index.unique && postings.get(&key).is_some_and(|keys| !keys.is_empty()) {
                return Err(RelationalError::Constraint(format!(
                    "unique index {} on table {table} has a duplicate key",
                    index.name
                )));
            }
            postings.insert_posting(key, primary_key.clone());
        }
        segment.indexes.insert(index.name, postings);
    }
    Ok(())
}

fn refresh_indexes_for_keys(
    previous: &RelationalState,
    next: &mut RelationalState,
    table: &str,
    keys: &BTreeSet<RelationalKey>,
) -> Result<(), RelationalError> {
    let schema = Arc::clone(
        next.schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    let mut index_schemas = schema.indexes.clone();
    for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
        index_schemas.push(RelationalIndexSchema {
            name: format!("__unique_{ordinal}"),
            columns: columns.clone(),
            unique: true,
        });
    }
    let previous_segment = previous.segments.get(table);
    let next_segment = next
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let next_segment = Arc::make_mut(next_segment);

    for index in index_schemas {
        let positions = column_positions(&schema, &index.columns)?;
        let postings = next_segment.indexes.get_mut(&index.name).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "table {table} is missing materialized index {}",
                index.name
            ))
        })?;
        for primary_key in keys {
            if let Some(previous_row) =
                previous_segment.and_then(|segment| segment.rows.get(primary_key))
            {
                let index_key = row_key(previous_row, &positions);
                postings.remove_posting(&index_key, primary_key);
            }
        }
        for primary_key in keys {
            let Some(row) = next_segment.rows.get(primary_key) else {
                continue;
            };
            let index_key = row_key(row, &positions);
            if index.unique
                && index_key
                    .0
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Null))
            {
                continue;
            }
            if index.unique
                && postings
                    .get(&index_key)
                    .into_iter()
                    .flat_map(RelationalKeySetPages::iter)
                    .any(|existing| existing != primary_key)
            {
                return Err(RelationalError::Constraint(format!(
                    "unique index {} on table {table} has a duplicate key",
                    index.name
                )));
            }
            postings.insert_posting(index_key, primary_key.clone());
        }
    }
    Ok(())
}

fn validate_foreign_keys(state: &RelationalState) -> Result<(), RelationalError> {
    for (table, schema) in &state.schemas {
        let Some(segment) = state.segments.get(table) else {
            continue;
        };
        for foreign_key in &schema.foreign_keys {
            let local_positions = column_positions(schema, &foreign_key.columns)?;
            let referenced_schema = state
                .schemas
                .get(&foreign_key.referenced_table)
                .ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "foreign key references unknown table {}",
                        foreign_key.referenced_table
                    ))
                })?;
            let referenced_positions =
                column_positions(referenced_schema, &foreign_key.referenced_columns)?;
            let references_unique_key =
                foreign_key.referenced_columns == referenced_schema.primary_key
                    || referenced_schema
                        .unique_constraints
                        .iter()
                        .any(|columns| columns == &foreign_key.referenced_columns)
                    || referenced_schema.indexes.iter().any(|index| {
                        index.unique && index.columns == foreign_key.referenced_columns
                    });
            if !references_unique_key {
                return Err(RelationalError::Schema(format!(
                    "foreign key from {table} must reference a primary or unique key on {}",
                    foreign_key.referenced_table
                )));
            }
            for (local, referenced) in local_positions.iter().zip(&referenced_positions) {
                if schema.columns[*local].scalar_type
                    != referenced_schema.columns[*referenced].scalar_type
                {
                    return Err(RelationalError::Schema(format!(
                        "foreign key from {table} to {} has incompatible column types",
                        foreign_key.referenced_table
                    )));
                }
            }
            let referenced_rows = state
                .segments
                .get(&foreign_key.referenced_table)
                .ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "missing row segment for table {}",
                        foreign_key.referenced_table
                    ))
                })?;
            let referenced_keys = referenced_rows
                .rows
                .values()
                .map(|row| row_key(row, &referenced_positions))
                .collect::<BTreeSet<_>>();
            for row in segment.rows.values() {
                let key = row_key(row, &local_positions);
                if key
                    .0
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Null))
                {
                    continue;
                }
                if !referenced_keys.contains(&key) {
                    return Err(RelationalError::Constraint(format!(
                        "foreign key from {table} to {} has no visible target",
                        foreign_key.referenced_table
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_foreign_keys_incremental(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    full_validation_tables: &BTreeSet<String>,
) -> Result<(), RelationalError> {
    for (table, schema) in &next.schemas {
        let Some(segment) = next.segments.get(table) else {
            continue;
        };
        for foreign_key in &schema.foreign_keys {
            let local_positions = column_positions(schema, &foreign_key.columns)?;
            let referenced_schema =
                next.schemas
                    .get(&foreign_key.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key references unknown table {}",
                            foreign_key.referenced_table
                        ))
                    })?;
            let referenced_positions =
                column_positions(referenced_schema, &foreign_key.referenced_columns)?;
            validate_foreign_key_shape(
                table,
                schema,
                foreign_key,
                referenced_schema,
                &local_positions,
                &referenced_positions,
            )?;

            if full_validation_tables.contains(table) {
                for row in segment.rows.values() {
                    validate_foreign_key_row(next, table, row, foreign_key, &local_positions)?;
                }
            } else if let Some(keys) = changed_keys.get(table) {
                for key in keys {
                    if let Some(row) = segment.rows.get(key) {
                        validate_foreign_key_row(next, table, row, foreign_key, &local_positions)?;
                    }
                }
            }

            let Some(referenced_changes) = changed_keys.get(&foreign_key.referenced_table) else {
                continue;
            };
            let previous_referenced = previous.segments.get(&foreign_key.referenced_table);
            for changed_key in referenced_changes {
                let Some(previous_row) =
                    previous_referenced.and_then(|segment| segment.rows.get(changed_key))
                else {
                    continue;
                };
                let previous_target = row_key(previous_row, &referenced_positions);
                if foreign_key_target_exists(next, foreign_key, &previous_target)? {
                    continue;
                }
                if segment.rows.values().any(|row| {
                    let local_key = row_key(row, &local_positions);
                    !key_contains_null(&local_key) && local_key == previous_target
                }) {
                    return Err(RelationalError::Constraint(format!(
                        "foreign key from {table} to {} prevents removing a visible target",
                        foreign_key.referenced_table
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_foreign_key_shape(
    table: &str,
    schema: &RelationalTableSchema,
    foreign_key: &RelationalForeignKeySchema,
    referenced_schema: &RelationalTableSchema,
    local_positions: &[usize],
    referenced_positions: &[usize],
) -> Result<(), RelationalError> {
    let references_unique_key = foreign_key.referenced_columns == referenced_schema.primary_key
        || referenced_schema
            .unique_constraints
            .iter()
            .any(|columns| columns == &foreign_key.referenced_columns)
        || referenced_schema
            .indexes
            .iter()
            .any(|index| index.unique && index.columns == foreign_key.referenced_columns);
    if !references_unique_key {
        return Err(RelationalError::Schema(format!(
            "foreign key from {table} must reference a primary or unique key on {}",
            foreign_key.referenced_table
        )));
    }
    for (local, referenced) in local_positions.iter().zip(referenced_positions) {
        if schema.columns[*local].scalar_type != referenced_schema.columns[*referenced].scalar_type
        {
            return Err(RelationalError::Schema(format!(
                "foreign key from {table} to {} has incompatible column types",
                foreign_key.referenced_table
            )));
        }
    }
    Ok(())
}

fn validate_foreign_key_row(
    state: &RelationalState,
    table: &str,
    row: &RelationalRow,
    foreign_key: &RelationalForeignKeySchema,
    local_positions: &[usize],
) -> Result<(), RelationalError> {
    let key = row_key(row, local_positions);
    if key_contains_null(&key) || foreign_key_target_exists(state, foreign_key, &key)? {
        return Ok(());
    }
    Err(RelationalError::Constraint(format!(
        "foreign key from {table} to {} has no visible target",
        foreign_key.referenced_table
    )))
}

fn foreign_key_target_exists(
    state: &RelationalState,
    foreign_key: &RelationalForeignKeySchema,
    key: &RelationalKey,
) -> Result<bool, RelationalError> {
    let schema = state
        .schemas
        .get(&foreign_key.referenced_table)
        .ok_or_else(|| {
            RelationalError::Schema(format!(
                "foreign key references unknown table {}",
                foreign_key.referenced_table
            ))
        })?;
    if foreign_key.referenced_columns == schema.primary_key {
        return Ok(state.row(&foreign_key.referenced_table, key).is_some());
    }
    if let Some(ordinal) = schema
        .unique_constraints
        .iter()
        .position(|columns| columns == &foreign_key.referenced_columns)
    {
        return Ok(state
            .index_lookup(
                &foreign_key.referenced_table,
                &format!("__unique_{ordinal}"),
                key,
            )
            .is_some_and(|postings| !postings.is_empty()));
    }
    if let Some(index) = schema
        .indexes
        .iter()
        .find(|index| index.unique && index.columns == foreign_key.referenced_columns)
    {
        return Ok(state
            .index_lookup(&foreign_key.referenced_table, &index.name, key)
            .is_some_and(|postings| !postings.is_empty()));
    }
    Err(RelationalError::Schema(format!(
        "foreign key references non-unique columns on {}",
        foreign_key.referenced_table
    )))
}

fn key_contains_null(key: &RelationalKey) -> bool {
    key.0
        .iter()
        .any(|value| matches!(value, RelationalValue::Null))
}

fn column_positions(
    schema: &RelationalTableSchema,
    columns: &[String],
) -> Result<Vec<usize>, RelationalError> {
    columns
        .iter()
        .map(|column| {
            schema.column_position(column).ok_or_else(|| {
                RelationalError::Schema(format!("table {} has no column {column}", schema.name))
            })
        })
        .collect()
}

fn row_key(row: &RelationalRow, positions: &[usize]) -> RelationalKey {
    RelationalKey(
        positions
            .iter()
            .map(|position| row.values[*position].clone())
            .collect(),
    )
}

#[cfg(test)]
#[path = "relational/tests.rs"]
mod tests;
