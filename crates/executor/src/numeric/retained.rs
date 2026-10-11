// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Immutable ownership of admitted numeric producer buffers.
//!
//! This is an executor building block, not a query cursor or foreign API. Source
//! values are constructed here; sealing and retaining do not copy their payload.

use super::lending::{NumericBatchValues, NumericValueBuffer};
use super::NumericFragment;
use crate::{QueryMemoryAccount, QueryMemoryLease, ValidityView};
use hawdb_core::{HawDBError, PropertyType, Result, Value};
use hawdb_qos::{RuntimePermit, RuntimeRetainedResult, RuntimeRetainedResultError};
use hawdb_storage::NodeRecord;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Keeps retryable admission separate from producer/query failures.
#[derive(Debug)]
pub enum RetainedNumericError {
    Admission(RuntimeRetainedResultError),
    Execution(HawDBError),
}

impl std::fmt::Display for RetainedNumericError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RetainedNumericError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Execution(error) => Some(error),
        }
    }
}

impl From<HawDBError> for RetainedNumericError {
    fn from(error: HawDBError) -> Self {
        Self::Execution(error)
    }
}

impl From<RuntimeRetainedResultError> for RetainedNumericError {
    fn from(error: RuntimeRetainedResultError) -> Self {
        Self::Admission(error)
    }
}

type RetainedResult<T> = std::result::Result<T, RetainedNumericError>;

static NEXT_ALLOCATION: AtomicU64 = AtomicU64::new(1);
static ALLOCATION_NAMESPACE: u8 = 0;
const BUFFER_PADDING_BYTES: usize = 64;

/// Allocation provenance within a loaded engine module. Fresh allocations
/// receive distinct identifiers even when an allocator recycles an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericBufferIdentity {
    pub namespace: u64,
    pub allocation: u64,
    pub generation: u64,
}

/// Visible range within one native allocation. Capacity remains charged even
/// when filtering/limit exposes a small or empty result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericBufferProvenance {
    pub identity: NumericBufferIdentity,
    pub retained_capacity_bytes: usize,
    pub byte_offset: usize,
    pub byte_length: usize,
}

fn provenance<T>(values: &Vec<T>, identity: NumericBufferIdentity) -> NumericBufferProvenance {
    NumericBufferProvenance {
        identity,
        retained_capacity_bytes: values.capacity() * std::mem::size_of::<T>(),
        byte_offset: 0,
        byte_length: values.len() * std::mem::size_of::<T>(),
    }
}

fn buffer_provenance(
    values: &NumericValueBuffer,
    node_ids: &Option<Vec<u64>>,
    validity: &Vec<u64>,
    selection: &Vec<u32>,
    identities: [NumericBufferIdentity; 4],
) -> [Option<NumericBufferProvenance>; 4] {
    let values = match values {
        NumericValueBuffer::Int(values) => provenance(values, identities[0]),
        NumericValueBuffer::Float(values) => provenance(values, identities[0]),
    };
    [
        Some(values),
        node_ids.as_ref().map(|ids| provenance(ids, identities[1])),
        (!validity.is_empty()).then(|| provenance(validity, identities[2])),
        Some(provenance(selection, identities[3])),
    ]
}

impl NumericBufferIdentity {
    fn new() -> Result<Self> {
        let allocation = NEXT_ALLOCATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| HawDBError::Execution("numeric buffer identity exhausted".into()))?;
        Ok(Self {
            namespace: std::ptr::addr_of!(ALLOCATION_NAMESPACE) as usize as u64,
            allocation,
            generation: 0,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RetainedNumericValues<'a> {
    Int(&'a [i64]),
    Float(&'a [f64]),
}

/// Prepaid owner metadata, released after the final immutable payload.
pub trait NumericBatchOwner: std::fmt::Debug + Send + Sync {}

/// Mutable first representation. No row maps or `Vec<Value>` are staged here.
#[derive(Debug)]
pub struct RetainedNumericBuilder<'plan> {
    fragment: NumericFragment<'plan>,
    capacity: usize,
    values: NumericValueBuffer,
    node_ids: Option<Vec<u64>>,
    validity: Vec<u64>,
    selection: Vec<u32>,
    rows: usize,
    failed: bool,
    identities: [NumericBufferIdentity; 4],
    owner: Option<Arc<dyn NumericBatchOwner>>,
    owner_metadata_capacity: usize,
    account: QueryMemoryAccount,
    memory: QueryMemoryLease,
    runtime: RuntimeRetainedResult,
}

#[derive(Debug)]
struct NumericStorage {
    values: NumericValueBuffer,
    node_ids: Option<Vec<u64>>,
    validity: Vec<u64>,
    selection: Vec<u32>,
    rows: usize,
    identities: [NumericBufferIdentity; 4],
    // Drop after payload/selection and before their capacity reservations.
    _owner: Option<Arc<dyn NumericBatchOwner>>,
    account: QueryMemoryAccount,
    // Drop payload before releasing its query-ledger charge.
    _memory: QueryMemoryLease,
}

/// Immutable buffers and one admitted view handle. There is deliberately no
/// `Clone`; every additional owned view must reserve its metadata first.
#[derive(Debug)]
pub struct RetainedNumericBatch {
    // Drop the final storage owner before its runtime charge.
    storage: Arc<NumericStorage>,
    _view_memory: QueryMemoryLease,
    runtime: RuntimeRetainedResult,
}

impl<'plan> RetainedNumericBuilder<'plan> {
    pub fn required_capacity_bytes(rows: NonZeroUsize, needs_node_ids: bool) -> Option<usize> {
        let rows = rows.get();
        if rows > u32::MAX as usize {
            return None;
        }
        let fixed = rows.checked_mul(12 + usize::from(needs_node_ids) * 8)?;
        let validity = rows.div_ceil(64).checked_mul(8)?;
        fixed
            .checked_add(validity)?
            .checked_add(std::mem::size_of::<NumericStorage>())?
            .checked_add(2 * std::mem::size_of::<usize>())?
            .checked_add((3 + usize::from(needs_node_ids)) * BUFFER_PADDING_BYTES)
    }

    /// Reserves both query and aggregate retained memory before allocating.
    /// Native capacities include conservative padding and the owner layout.
    pub fn new(
        fragment: NumericFragment<'plan>,
        rows: NonZeroUsize,
        needs_node_ids: bool,
        account: QueryMemoryAccount,
        permit: &RuntimePermit,
    ) -> RetainedResult<Self> {
        Self::new_with_metadata(fragment, rows, needs_node_ids, account, permit, 0, 0)
    }

    /// Adapter-owned unique metadata stays charged with the buffer owner;
    /// independently owned view metadata stays charged with each view.
    pub fn new_with_metadata(
        fragment: NumericFragment<'plan>,
        rows: NonZeroUsize,
        needs_node_ids: bool,
        account: QueryMemoryAccount,
        permit: &RuntimePermit,
        owner_metadata_bytes: usize,
        view_metadata_bytes: usize,
    ) -> RetainedResult<Self> {
        if !matches!(
            fragment.property_type,
            PropertyType::Int | PropertyType::Float
        ) {
            return Err(HawDBError::Execution("unsupported retained numeric type".into()).into());
        }
        let bytes = Self::required_capacity_bytes(rows, needs_node_ids)
            .and_then(|bytes| bytes.checked_add(owner_metadata_bytes))
            .ok_or_else(|| HawDBError::Execution("retained numeric size overflow".into()))?;
        let view_bytes = std::mem::size_of::<RetainedNumericBatch>()
            .checked_add(view_metadata_bytes)
            .ok_or_else(|| HawDBError::Execution("retained numeric size overflow".into()))?;
        let runtime = permit.reserve_retained_result(bytes as u64, view_bytes as u64)?;
        let native_overhead =
            RuntimeRetainedResult::owner_overhead_bytes() + runtime.handle_bytes();
        let memory = account.reserve(
            bytes
                .checked_add(native_overhead as usize)
                .ok_or_else(|| HawDBError::Execution("retained numeric size overflow".into()))?,
        )?;
        let identities = [
            NumericBufferIdentity::new()?,
            NumericBufferIdentity::new()?,
            NumericBufferIdentity::new()?,
            NumericBufferIdentity::new()?,
        ];
        Ok(Self {
            fragment,
            capacity: rows.get(),
            values: NumericValueBuffer::with_capacity(fragment.property_type, rows.get()),
            node_ids: needs_node_ids.then(|| Vec::with_capacity(rows.get())),
            validity: Vec::new(),
            selection: Vec::with_capacity(rows.get()),
            rows: 0,
            failed: false,
            identities,
            owner: None,
            owner_metadata_capacity: owner_metadata_bytes,
            account,
            memory,
            runtime,
        })
    }

    /// Attach metadata only after its unique allocation has been prepaid.
    pub fn attach_owner<T: NumericBatchOwner + 'static>(&mut self, owner: Arc<T>) -> Result<()> {
        let bytes = std::mem::size_of::<T>() + 2 * std::mem::size_of::<usize>();
        if self.owner.is_some() || bytes > self.owner_metadata_capacity {
            self.failed = true;
            return Err(HawDBError::Execution(
                "numeric owner metadata was not admitted".into(),
            ));
        }
        self.owner = Some(owner);
        Ok(())
    }

    pub fn push_node(&mut self, node: &NodeRecord) -> Result<()> {
        if self.failed || self.rows >= self.capacity {
            self.failed = true;
            return Err(HawDBError::Execution(
                "retained numeric producer capacity exceeded or failed".into(),
            ));
        }
        // Do not format a corrupt, potentially large property into an error.
        // Only supported scalar types enter the existing numeric producer.
        let value = node.properties.get(self.fragment.property);
        if !matches!(
            (self.fragment.property_type, value),
            (_, None | Some(Value::Null))
                | (PropertyType::Int, Some(Value::Int(_)))
                | (PropertyType::Float, Some(Value::Float(_)))
        ) {
            self.failed = true;
            return Err(HawDBError::Execution(
                "retained numeric schema/value mismatch".into(),
            ));
        }
        let valid = match self.values.push_node_value(self.fragment, node) {
            Ok(valid) => valid,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        if let Some(node_ids) = &mut self.node_ids {
            node_ids.push(node.id.0);
        }
        if !valid && self.validity.is_empty() {
            // Exact allocation avoids Vec's amortized minimum/growth exceeding
            // the prepaid mask capacity for tiny/all-null batches.
            self.validity = Vec::with_capacity(self.capacity.div_ceil(64));
            self.validity.resize(self.rows.div_ceil(64), u64::MAX);
            if !self.rows.is_multiple_of(64) {
                *self.validity.last_mut().expect("partial validity word") =
                    (1u64 << (self.rows % 64)) - 1;
            }
        }
        if self.validity.capacity() != 0 {
            let word = self.rows / 64;
            if self.validity.len() <= word {
                self.validity.push(0);
            }
            if valid {
                self.validity[word] |= 1u64 << (self.rows % 64);
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Identity before sealing, used to prove handoff preserves allocations.
    pub fn values(&self) -> (RetainedNumericValues<'_>, NumericBufferIdentity) {
        (values_view(&self.values), self.identities[0])
    }

    /// Values, optional node IDs, optional validity, and selection, in order.
    pub fn buffer_provenance(&self) -> [Option<NumericBufferProvenance>; 4] {
        buffer_provenance(
            &self.values,
            &self.node_ids,
            &self.validity,
            &self.selection,
            self.identities,
        )
    }

    /// Filtering creates only the prepaid selection; sealing moves each Vec
    /// into an Arc-owned structure without converting it to `Arc<[T]>`.
    pub fn seal(mut self, selected_limit: Option<usize>) -> RetainedResult<RetainedNumericBatch> {
        if self.failed {
            return Err(HawDBError::Execution("retained numeric producer failed".into()).into());
        }
        let validity = validity_view(self.rows, &self.validity);
        match self.values.view() {
            NumericBatchValues::Int(values) => {
                self.fragment
                    .select_int64_values(values, validity, &mut self.selection)?
            }
            NumericBatchValues::Float(values) => {
                self.fragment
                    .select_float64_values(values, validity, &mut self.selection)?
            }
        }
        if let Some(limit) = selected_limit {
            self.selection.truncate(limit);
        }
        // The handle portion is separate from shared storage in the query ledger.
        let view_bytes = self.runtime.handle_bytes() as usize;
        let view_memory = self.memory.split_off(view_bytes)?;
        Ok(RetainedNumericBatch {
            storage: Arc::new(NumericStorage {
                values: self.values,
                node_ids: self.node_ids,
                validity: self.validity,
                selection: self.selection,
                rows: self.rows,
                identities: self.identities,
                _owner: self.owner,
                account: self.account,
                _memory: self.memory,
            }),
            _view_memory: view_memory,
            runtime: self.runtime,
        })
    }
}

fn values_view(values: &NumericValueBuffer) -> RetainedNumericValues<'_> {
    match values.view() {
        NumericBatchValues::Int(values) => RetainedNumericValues::Int(values),
        NumericBatchValues::Float(values) => RetainedNumericValues::Float(values),
    }
}

fn validity_view(rows: usize, words: &[u64]) -> ValidityView<'_> {
    if words.is_empty() {
        ValidityView::All { len: rows }
    } else {
        ValidityView::Bitmap { len: rows, words }
    }
}

impl RetainedNumericBatch {
    /// Values, optional node IDs, optional validity, and selection, in order.
    pub fn buffer_provenance(&self) -> [Option<NumericBufferProvenance>; 4] {
        buffer_provenance(
            &self.storage.values,
            &self.storage.node_ids,
            &self.storage.validity,
            &self.storage.selection,
            self.storage.identities,
        )
    }

    pub fn physical_rows(&self) -> usize {
        self.storage.rows
    }

    pub fn selected_rows(&self) -> (&[u32], NumericBufferIdentity) {
        (&self.storage.selection, self.storage.identities[3])
    }

    pub fn values(&self) -> (RetainedNumericValues<'_>, NumericBufferIdentity) {
        (
            values_view(&self.storage.values),
            self.storage.identities[0],
        )
    }

    pub fn node_ids(&self) -> Option<(&[u64], NumericBufferIdentity)> {
        self.storage
            .node_ids
            .as_deref()
            .map(|ids| (ids, self.storage.identities[1]))
    }

    pub fn validity(&self) -> ValidityView<'_> {
        validity_view(self.storage.rows, &self.storage.validity)
    }

    pub fn validity_buffer(&self) -> Option<(&[u64], NumericBufferIdentity)> {
        (!self.storage.validity.is_empty())
            .then_some((&self.storage.validity, self.storage.identities[2]))
    }

    /// Shares every payload/selection allocation while admitting a fresh handle.
    /// Caller metadata includes any adapter-owned descriptors/wrappers.
    pub fn try_retain(&self, adapter_metadata_bytes: usize) -> RetainedResult<Self> {
        let metadata = std::mem::size_of::<Self>()
            .checked_add(adapter_metadata_bytes)
            .ok_or_else(|| HawDBError::Execution("retained numeric metadata overflow".into()))?;
        let runtime = self.runtime.try_retain(metadata as u64)?;
        let view_memory = self
            .storage
            .account
            .reserve(runtime.handle_bytes() as usize)?;
        Ok(Self {
            storage: Arc::clone(&self.storage),
            _view_memory: view_memory,
            runtime,
        })
    }

    pub fn source_constructed_bytes(&self) -> usize {
        self.storage.rows * (8 + usize::from(self.storage.node_ids.is_some()) * 8)
    }
}

#[cfg(test)]
mod tests;
