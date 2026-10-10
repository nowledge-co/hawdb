// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Experimental C views over the root retained numeric producer.
//! Source/planning workspace and whole-operation qualification remain incomplete.
//! Every exposed payload is immutable. Raw pointers expire with their owner handle.

use super::{parse_cypher_params, read_str, FfiStr, HawdbDatabase};
use hawdb::{
    RetainedBufferProvenance, RetainedColumnRole, RetainedColumnType, RetainedColumnValues,
    RetainedQueryBatch, RetainedQueryCursor, RetainedQueryError, RetainedQueryOptions,
    RetainedValidityView,
};
use std::ffi::{c_char, c_void};
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const HAWDB_RETAINED_ABI_V1: u32 = 1;
pub const HAWDB_RETAINED_OK: u32 = 0;
pub const HAWDB_RETAINED_EOF: u32 = 1;
pub const HAWDB_RETAINED_BACKPRESSURE: u32 = 2;
pub const HAWDB_RETAINED_INVALID_HANDLE: u32 = 3;
pub const HAWDB_RETAINED_INVALID_ARGUMENT: u32 = 4;
pub const HAWDB_RETAINED_PANIC: u32 = 5;
pub const HAWDB_RETAINED_CLOSED: u32 = 6;
pub const HAWDB_RETAINED_WORKING_UNIT_TOO_LARGE: u32 = 7;
pub const HAWDB_RETAINED_EXECUTION_ERROR: u32 = 8;
pub const HAWDB_RETAINED_RESULT_BUDGET: u32 = 9;
pub const HAWDB_RETAINED_UNSUPPORTED_PLAN: u32 = 10;
pub const HAWDB_RETAINED_UNSUPPORTED_LAYOUT: u32 = 11;
pub const HAWDB_RETAINED_UNSUPPORTED_TYPE: u32 = 12;
pub const HAWDB_RETAINED_COPY_REQUIRED: u32 = 13;
pub const HAWDB_RETAINED_SELECTION_REQUIRES_MATERIALIZATION: u32 = 14;
pub const HAWDB_RETAINED_INVALID_COLUMN: u32 = 15;
pub const HAWDB_RETAINED_SIZE_OVERFLOW: u32 = 16;
pub const HAWDB_RETAINED_STOPPED: u32 = 17;
pub const HAWDB_RETAINED_ADMISSION_ERROR: u32 = 18;
pub const HAWDB_RETAINED_INT64: u32 = 1;
pub const HAWDB_RETAINED_FLOAT64: u32 = 2;
pub const HAWDB_RETAINED_UINT64: u32 = 3;
pub const HAWDB_RETAINED_PROPERTY: u32 = 1;
pub const HAWDB_RETAINED_NODE_IDENTITY: u32 = 2;
pub const HAWDB_RETAINED_READ_ONLY: u32 = 1;
pub const HAWDB_RETAINED_BORROWED: u32 = 2;
pub const HAWDB_RETAINED_REQUIRE_SOURCE_REUSE: u32 = 1;
pub const HAWDB_RETAINED_REQUEST_WRITABLE: u32 = 2;
pub const HAWDB_RETAINED_VALIDITY_ALL: u32 = 1;
pub const HAWDB_RETAINED_VALIDITY_U64_LSB: u32 = 2;

type Result<T> = std::result::Result<T, u32>;
static HANDLE_NAMESPACE: u8 = 0;
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// All lengths are byte lengths. A zero batch/slot limit selects the Rust
/// default. Unknown flags/versions refuse before ownership transfer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HawdbRetainedQueryV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub cypher: *const c_char,
    pub cypher_len: u64,
    pub params_json: *const c_char,
    pub params_len: u64,
    pub batch_rows: u32,
    pub outstanding_batches: u32,
    pub batch_bytes: u64,
    pub flags: u32,
    pub reserved: u32,
}

/// Borrowed immutable range. data addresses the visible range; byte_offset is
/// allocation provenance and must not be applied to data a second time. Keep
/// the owner live and library mapped. A small slice retains its full capacity.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedBufferV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub data: *const c_void,
    pub allocation_namespace: u64,
    pub allocation_id: u64,
    pub generation: u64,
    pub retained_capacity_bytes: u64,
    pub byte_offset: u64,
    pub byte_length: u64,
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedCursorV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub owner_namespace: u64,
    pub owner_id: u64,
    pub column_count: u64,
}

/// Selected indices address the physical rows, without gathering their values.
/// Release owner_id explicitly. EOF never carries a live batch owner.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedBatchV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub owner_namespace: u64,
    pub owner_id: u64,
    pub physical_rows: u64,
    pub selected_rows: u64,
    pub column_count: u64,
    pub selection: HawdbRetainedBufferV1,
}

/// Schema names borrow the supplied cursor/batch/column handle. They are UTF-8
/// byte ranges, without a NUL-termination contract.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedSchemaV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub name: *const c_char,
    pub name_len: u64,
    pub data_type: u32,
    pub role: u32,
    pub nullable: u32,
    pub flags: u32,
}

/// Independently owned read-only column, including its selection and validity.
/// The validity bitmap consists of native u64 words; row r uses bit r % 64.
/// Release this owner even when its parent batch was already released.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedColumnV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub owner_namespace: u64,
    pub owner_id: u64,
    pub physical_rows: u64,
    pub selected_rows: u64,
    pub schema: HawdbRetainedSchemaV1,
    pub values: HawdbRetainedBufferV1,
    pub selection: HawdbRetainedBufferV1,
    pub validity: HawdbRetainedBufferV1,
    pub validity_kind: u32,
    pub flags: u32,
}

/// Status is 0 Open, 1 Completed, 2 Failed or 3 Closed. Earlier views remain
/// readable after failure/close; only Completed makes the result final.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedStateV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub status: u32,
    pub terminal_code: u32,
    pub visited_rows: u64,
    pub emitted_rows: u64,
    pub source_constructed_bytes: u64,
    pub source_pinned_rows: u64,
    pub source_pinned_pages: u64,
}

struct CursorState {
    cursor: RetainedQueryCursor,
    terminal_code: u32,
}
enum Object {
    Cursor(Box<Mutex<CursorState>>),
    Batch(RetainedQueryBatch),
    Column {
        batch: RetainedQueryBatch,
        index: usize,
    },
}
struct HandleRecord {
    id: u64,
    object: Arc<Object>,
    next: Option<Box<HandleRecord>>,
}
#[derive(Default)]
struct Registry {
    head: Option<Box<HandleRecord>>,
}
impl Registry {
    fn get(&self, id: u64) -> Result<Arc<Object>> {
        let mut next = self.head.as_deref();
        while let Some(record) = next {
            if record.id == id {
                return Ok(Arc::clone(&record.object));
            }
            next = record.next.as_deref();
        }
        Err(HAWDB_RETAINED_INVALID_HANDLE)
    }
    fn remove(&mut self, id: u64) -> Result<Arc<Object>> {
        let mut next = &mut self.head;
        loop {
            if next.as_ref().is_some_and(|record| record.id == id) {
                let mut record = next.take().expect("matched record");
                *next = record.next.take();
                return Ok(record.object);
            }
            next = &mut next.as_mut().ok_or(HAWDB_RETAINED_INVALID_HANDLE)?.next;
        }
    }
}
// Registry nodes have no unused vector/tree capacity. Each node and Arc object
// is prepaid by its originating root owner before allocation.
static REGISTRY: Mutex<Registry> = Mutex::new(Registry { head: None });

fn namespace() -> u64 {
    &HANDLE_NAMESPACE as *const u8 as usize as u64
}
fn next_id() -> Result<u64> {
    NEXT_HANDLE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| HAWDB_RETAINED_SIZE_OVERFLOW)
}
fn metadata_bytes(descriptor_bytes: usize) -> usize {
    std::mem::size_of::<HandleRecord>()
        + std::mem::size_of::<Object>()
        + 2 * std::mem::size_of::<usize>()
        + descriptor_bytes
        + 128
}
fn lookup(owner_namespace: u64, id: u64) -> Result<Arc<Object>> {
    if owner_namespace != namespace() || id == 0 {
        return Err(HAWDB_RETAINED_INVALID_HANDLE);
    }
    REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(id)
}
fn register(id: u64, object: Object) {
    let mut record = Box::new(HandleRecord {
        id,
        object: Arc::new(object),
        next: None,
    });
    let mut registry = REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    record.next = registry.head.take();
    registry.head = Some(record);
}
fn error_code(error: &RetainedQueryError) -> u32 {
    if error.is_retryable() {
        return HAWDB_RETAINED_BACKPRESSURE;
    }
    match error {
        RetainedQueryError::UnsupportedPlan => HAWDB_RETAINED_UNSUPPORTED_PLAN,
        RetainedQueryError::UnsupportedLayout => HAWDB_RETAINED_UNSUPPORTED_LAYOUT,
        RetainedQueryError::UnsupportedType => HAWDB_RETAINED_UNSUPPORTED_TYPE,
        RetainedQueryError::CopyRequired => HAWDB_RETAINED_COPY_REQUIRED,
        RetainedQueryError::SelectionRequiresMaterialization => {
            HAWDB_RETAINED_SELECTION_REQUIRES_MATERIALIZATION
        }
        RetainedQueryError::WorkingUnitTooLarge => HAWDB_RETAINED_WORKING_UNIT_TOO_LARGE,
        RetainedQueryError::Admission(_) => HAWDB_RETAINED_ADMISSION_ERROR,
        RetainedQueryError::RetainedAdmission(error) => match error {
            hawdb::RuntimeRetainedResultError::Capacity { .. } => {
                HAWDB_RETAINED_WORKING_UNIT_TOO_LARGE
            }
            hawdb::RuntimeRetainedResultError::SizeOverflow => HAWDB_RETAINED_SIZE_OVERFLOW,
            hawdb::RuntimeRetainedResultError::Admission(_) => HAWDB_RETAINED_ADMISSION_ERROR,
        },
        RetainedQueryError::ResultBudget { .. } => HAWDB_RETAINED_RESULT_BUDGET,
        RetainedQueryError::Stopped(_) => HAWDB_RETAINED_STOPPED,
        RetainedQueryError::Closed => HAWDB_RETAINED_CLOSED,
        RetainedQueryError::InvalidColumn => HAWDB_RETAINED_INVALID_COLUMN,
        RetainedQueryError::SizeOverflow => HAWDB_RETAINED_SIZE_OVERFLOW,
        _ => HAWDB_RETAINED_EXECUTION_ERROR,
    }
}
fn caught(operation: impl FnOnce() -> Result<()>) -> u32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => HAWDB_RETAINED_OK,
        Ok(Err(code)) => code,
        Err(_) => HAWDB_RETAINED_PANIC,
    }
}
unsafe fn output<T: Default>(
    out: *mut T,
    out_size: u32,
    operation: impl FnOnce() -> Result<T>,
) -> u32 {
    caught(|| {
        if out.is_null() {
            return Err(HAWDB_RETAINED_INVALID_ARGUMENT);
        }
        let written = (out_size as usize).min(std::mem::size_of::<T>());
        unsafe { ptr::write_bytes(out.cast::<u8>(), 0, written) };
        if (out_size as usize) < std::mem::size_of::<T>() {
            return Err(HAWDB_RETAINED_INVALID_ARGUMENT);
        }
        unsafe { out.write(T::default()) };
        let value = operation()?;
        unsafe { out.write(value) };
        Ok(())
    })
}
fn bytes_len(len: u64) -> Result<usize> {
    usize::try_from(len)
        .ok()
        .filter(|len| *len <= isize::MAX as usize)
        .ok_or(HAWDB_RETAINED_INVALID_ARGUMENT)
}
fn buffer(data: *const c_void, provenance: RetainedBufferProvenance) -> HawdbRetainedBufferV1 {
    HawdbRetainedBufferV1 {
        abi_version: HAWDB_RETAINED_ABI_V1,
        struct_size: std::mem::size_of::<HawdbRetainedBufferV1>() as u32,
        data: if provenance.byte_length == 0 {
            ptr::null()
        } else {
            data
        },
        allocation_namespace: provenance.identity.namespace,
        allocation_id: provenance.identity.allocation,
        generation: provenance.identity.generation,
        retained_capacity_bytes: provenance.retained_capacity_bytes as u64,
        byte_offset: provenance.byte_offset as u64,
        byte_length: provenance.byte_length as u64,
    }
}
fn batch_descriptor(batch: &RetainedQueryBatch, id: u64) -> HawdbRetainedBatchV1 {
    HawdbRetainedBatchV1 {
        abi_version: HAWDB_RETAINED_ABI_V1,
        struct_size: std::mem::size_of::<HawdbRetainedBatchV1>() as u32,
        owner_namespace: namespace(),
        owner_id: id,
        physical_rows: batch.physical_rows() as u64,
        selected_rows: batch.selected_rows().len() as u64,
        column_count: batch.schema().len() as u64,
        selection: buffer(
            batch.selected_rows().as_ptr().cast(),
            batch.selection_provenance(),
        ),
    }
}
fn schema_descriptor(column: &hawdb::RetainedColumnSchema) -> HawdbRetainedSchemaV1 {
    HawdbRetainedSchemaV1 {
        abi_version: HAWDB_RETAINED_ABI_V1,
        struct_size: std::mem::size_of::<HawdbRetainedSchemaV1>() as u32,
        name: column.name.as_ptr().cast(),
        name_len: column.name.len() as u64,
        data_type: match column.data_type {
            RetainedColumnType::Int64 => HAWDB_RETAINED_INT64,
            RetainedColumnType::Float64 => HAWDB_RETAINED_FLOAT64,
            RetainedColumnType::UInt64 => HAWDB_RETAINED_UINT64,
        },
        role: match column.role {
            RetainedColumnRole::Property => HAWDB_RETAINED_PROPERTY,
            RetainedColumnRole::NodeIdentity => HAWDB_RETAINED_NODE_IDENTITY,
        },
        nullable: u32::from(column.nullable),
        flags: HAWDB_RETAINED_READ_ONLY,
    }
}
fn column_descriptor(
    batch: &RetainedQueryBatch,
    index: usize,
    id: u64,
) -> Result<HawdbRetainedColumnV1> {
    let schema = batch
        .schema()
        .get(index)
        .ok_or(HAWDB_RETAINED_INVALID_COLUMN)?;
    let data = match batch.column(index).map_err(|error| error_code(&error))? {
        RetainedColumnValues::Int64(values) => values.as_ptr().cast(),
        RetainedColumnValues::Float64(values) => values.as_ptr().cast(),
        RetainedColumnValues::UInt64(values) => values.as_ptr().cast(),
    };
    let validity = match batch.validity(index).map_err(|error| error_code(&error))? {
        RetainedValidityView::All { .. } => HawdbRetainedBufferV1::default(),
        RetainedValidityView::Bitmap { words, .. } => buffer(
            words.as_ptr().cast(),
            batch
                .validity_provenance(index)
                .map_err(|error| error_code(&error))?
                .expect("bitmap provenance"),
        ),
    };
    Ok(HawdbRetainedColumnV1 {
        abi_version: HAWDB_RETAINED_ABI_V1,
        struct_size: std::mem::size_of::<HawdbRetainedColumnV1>() as u32,
        owner_namespace: namespace(),
        owner_id: id,
        physical_rows: batch.physical_rows() as u64,
        selected_rows: batch.selected_rows().len() as u64,
        schema: schema_descriptor(schema),
        values: buffer(
            data,
            batch
                .column_provenance(index)
                .map_err(|error| error_code(&error))?,
        ),
        selection: buffer(
            batch.selected_rows().as_ptr().cast(),
            batch.selection_provenance(),
        ),
        validity_kind: if validity.data.is_null() {
            HAWDB_RETAINED_VALIDITY_ALL
        } else {
            HAWDB_RETAINED_VALIDITY_U64_LSB
        },
        validity,
        flags: HAWDB_RETAINED_READ_ONLY,
    })
}

/// Return the additive experimental retained descriptor ABI version.
#[unsafe(no_mangle)]
pub extern "C" fn hawdb_retained_abi_version() -> u32 {
    HAWDB_RETAINED_ABI_V1
}

/// Create an eligible experimental cursor. There is no copying fallback or JSON
/// result encoding. Parameter JSON remains an input format.
///
/// # Safety
/// db must remain live for this call. request must be aligned/readable for
/// request_size bytes, with valid pointed-to input ranges. out must be aligned
/// and writable for out_size bytes. Keep the library loaded until final release.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_query(
    db: *mut HawdbDatabase,
    request: *const HawdbRetainedQueryV1,
    request_size: u32,
    out: *mut HawdbRetainedCursorV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            if db.is_null()
                || request.is_null()
                || (request_size as usize) < std::mem::size_of::<HawdbRetainedQueryV1>()
            {
                return Err(HAWDB_RETAINED_INVALID_ARGUMENT);
            }
            let request = &*request;
            if request.abi_version != HAWDB_RETAINED_ABI_V1
                || request.struct_size > request_size
                || (request.struct_size as usize) < std::mem::size_of::<HawdbRetainedQueryV1>()
                || request.flags
                    & !(HAWDB_RETAINED_REQUIRE_SOURCE_REUSE | HAWDB_RETAINED_REQUEST_WRITABLE)
                    != 0
                || request.reserved != 0
            {
                return Err(HAWDB_RETAINED_INVALID_ARGUMENT);
            }
            if request.flags & HAWDB_RETAINED_REQUEST_WRITABLE != 0 {
                return Err(HAWDB_RETAINED_COPY_REQUIRED);
            }
            let text = read_str(
                FfiStr {
                    ptr: request.cypher,
                    len: bytes_len(request.cypher_len)?,
                },
                "cypher",
            )
            .map_err(|_| HAWDB_RETAINED_INVALID_ARGUMENT)?;
            let params = parse_cypher_params(FfiStr {
                ptr: request.params_json,
                len: bytes_len(request.params_len)?,
            })
            .map_err(|_| HAWDB_RETAINED_INVALID_ARGUMENT)?;
            let mut options = RetainedQueryOptions::default();
            if let Some(rows) = NonZeroUsize::new(request.batch_rows as usize) {
                options.batch_rows = rows;
            }
            if let Some(slots) = NonZeroUsize::new(request.outstanding_batches as usize) {
                options.outstanding_batches = slots;
            }
            if request.batch_bytes != 0 {
                options.batch_bytes = NonZeroUsize::new(bytes_len(request.batch_bytes)?)
                    .ok_or(HAWDB_RETAINED_INVALID_ARGUMENT)?;
            }
            options.require_source_reuse = request.flags & HAWDB_RETAINED_REQUIRE_SOURCE_REUSE != 0;
            options.adapter_metadata_bytes = metadata_bytes(std::mem::size_of::<
                HawdbRetainedCursorV1,
            >()) + std::mem::size_of::<Mutex<CursorState>>()
                + 64;
            // Control, first batch and independently readable column each need a handle.
            options.minimum_shared_handles = NonZeroUsize::new(3).expect("nonzero handle minimum");
            let id = next_id()?;
            let cursor = (&*db)
                .inner
                .lock()
                .map_err(|_| HAWDB_RETAINED_PANIC)?
                .query_with_params_retained(text, &params, options)
                .map_err(|error| error_code(&error))?;
            let descriptor = HawdbRetainedCursorV1 {
                abi_version: HAWDB_RETAINED_ABI_V1,
                struct_size: std::mem::size_of::<HawdbRetainedCursorV1>() as u32,
                owner_namespace: namespace(),
                owner_id: id,
                column_count: cursor.schema().len() as u64,
            };
            register(
                id,
                Object::Cursor(Box::new(Mutex::new(CursorState {
                    cursor,
                    terminal_code: 0,
                }))),
            );
            Ok(descriptor)
        })
    }
}

/// Pull serially with no prefetch. Backpressure precedes source advancement and
/// EOF carries an empty output. Earlier batches remain provisional until EOF.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Do not release a handle
/// concurrently with access to its borrowed payload pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_next(
    owner_namespace: u64,
    owner_id: u64,
    out: *mut HawdbRetainedBatchV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let Object::Cursor(state) = object.as_ref() else {
                return Err(HAWDB_RETAINED_INVALID_HANDLE);
            };
            let id = next_id()?;
            let mut state = state.lock().map_err(|_| HAWDB_RETAINED_PANIC)?;
            if state.terminal_code != 0 {
                return Err(state.terminal_code);
            }
            let metadata = metadata_bytes(std::mem::size_of::<HawdbRetainedBatchV1>())
                .checked_add(
                    state
                        .cursor
                        .schema()
                        .len()
                        .checked_mul(std::mem::size_of::<HawdbRetainedColumnV1>())
                        .ok_or(HAWDB_RETAINED_SIZE_OVERFLOW)?,
                )
                .ok_or(HAWDB_RETAINED_SIZE_OVERFLOW)?;
            let result = catch_unwind(AssertUnwindSafe(|| {
                state.cursor.next_batch_with_metadata(metadata)
            }));
            let batch = match result {
                Ok(Ok(Some(batch))) => batch,
                Ok(Ok(None)) => return Err(HAWDB_RETAINED_EOF),
                Ok(Err(error)) => {
                    let code = error_code(&error);
                    if !error.is_retryable() {
                        state.terminal_code = code;
                    }
                    return Err(code);
                }
                Err(_) => {
                    state.terminal_code = HAWDB_RETAINED_PANIC;
                    state.cursor.abort_delivery();
                    return Err(HAWDB_RETAINED_PANIC);
                }
            };
            let descriptor = batch_descriptor(&batch, id);
            register(id, Object::Batch(batch));
            Ok(descriptor)
        })
    }
}

/// Return borrowed schema, including before the first pull or after empty EOF.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Schema names expire when
/// the supplied owner is released; retain its owner during all reads.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_schema(
    owner_namespace: u64,
    owner_id: u64,
    column: u64,
    out: *mut HawdbRetainedSchemaV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let index = usize::try_from(column).map_err(|_| HAWDB_RETAINED_INVALID_COLUMN)?;
            match object.as_ref() {
                Object::Cursor(state) => {
                    let state = state.lock().map_err(|_| HAWDB_RETAINED_PANIC)?;
                    state
                        .cursor
                        .schema()
                        .get(index)
                        .map(schema_descriptor)
                        .ok_or(HAWDB_RETAINED_INVALID_COLUMN)
                }
                Object::Batch(batch) => batch
                    .schema()
                    .get(index)
                    .map(schema_descriptor)
                    .ok_or(HAWDB_RETAINED_INVALID_COLUMN),
                Object::Column {
                    batch,
                    index: own_index,
                } => {
                    if index != *own_index {
                        return Err(HAWDB_RETAINED_INVALID_COLUMN);
                    }
                    Ok(schema_descriptor(&batch.schema()[index]))
                }
            }
        })
    }
}

/// Create an independent immutable column owner without copying payload.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Pointers remain readable
/// until this returned column owner is released, including after parent close.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_column(
    owner_namespace: u64,
    owner_id: u64,
    column: u64,
    out: *mut HawdbRetainedColumnV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let Object::Batch(batch) = object.as_ref() else {
                return Err(HAWDB_RETAINED_INVALID_HANDLE);
            };
            let index = usize::try_from(column).map_err(|_| HAWDB_RETAINED_INVALID_COLUMN)?;
            if index >= batch.schema().len() {
                return Err(HAWDB_RETAINED_INVALID_COLUMN);
            }
            let id = next_id()?;
            let batch = batch
                .try_retain(metadata_bytes(std::mem::size_of::<HawdbRetainedColumnV1>()))
                .map_err(|error| error_code(&error))?;
            let descriptor = column_descriptor(&batch, index, id)?;
            register(id, Object::Column { batch, index });
            Ok(descriptor)
        })
    }
}

/// Borrow a column from a batch without allocating or admitting another owner.
/// Its descriptor capacity was prepaid during the pull. This permits consuming
/// a held batch even when no additional independently owned view can be admitted.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. The supplied batch must
/// remain live throughout every pointer read. A BORROWED descriptor must not be
/// released independently; its owner_id refers to the existing batch.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_column_borrow(
    owner_namespace: u64,
    owner_id: u64,
    column: u64,
    out: *mut HawdbRetainedColumnV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let Object::Batch(batch) = object.as_ref() else {
                return Err(HAWDB_RETAINED_INVALID_HANDLE);
            };
            let index = usize::try_from(column).map_err(|_| HAWDB_RETAINED_INVALID_COLUMN)?;
            let mut descriptor = column_descriptor(batch, index, owner_id)?;
            descriptor.flags |= HAWDB_RETAINED_BORROWED;
            Ok(descriptor)
        })
    }
}

/// Retain an independent batch owner. Cursor and column handles refuse; columns
/// have their own export function and release lifetime.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Do not release its returned
/// owner until all reads through the descriptor have finished.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_batch_retain(
    owner_namespace: u64,
    owner_id: u64,
    out: *mut HawdbRetainedBatchV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let Object::Batch(batch) = object.as_ref() else {
                return Err(HAWDB_RETAINED_INVALID_HANDLE);
            };
            let id = next_id()?;
            let batch = batch
                .try_retain(metadata_bytes(std::mem::size_of::<HawdbRetainedBatchV1>()))
                .map_err(|error| error_code(&error))?;
            let descriptor = batch_descriptor(&batch, id);
            register(id, Object::Batch(batch));
            Ok(descriptor)
        })
    }
}

/// Retain an independent column owner, preserving all allocation identities and
/// ranges. Admission failure leaves the original column readable.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Release this new owner after
/// all reads, independently of the original column's owner.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_column_retain(
    owner_namespace: u64,
    owner_id: u64,
    out: *mut HawdbRetainedColumnV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let Object::Column { batch, index } = object.as_ref() else {
                return Err(HAWDB_RETAINED_INVALID_HANDLE);
            };
            let id = next_id()?;
            let retained = batch
                .try_retain(metadata_bytes(std::mem::size_of::<HawdbRetainedColumnV1>()))
                .map_err(|error| error_code(&error))?;
            let descriptor = column_descriptor(&retained, *index, id)?;
            register(
                id,
                Object::Column {
                    batch: retained,
                    index: *index,
                },
            );
            Ok(descriptor)
        })
    }
}

/// Read current terminal state. A prior descriptor's scalar status must not be
/// treated as final; this function observes late failures through retained views.
///
/// # Safety
/// out must be aligned/writable for out_size bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_state(
    owner_namespace: u64,
    owner_id: u64,
    out: *mut HawdbRetainedStateV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let mut descriptor = HawdbRetainedStateV1 {
                abi_version: HAWDB_RETAINED_ABI_V1,
                struct_size: std::mem::size_of::<HawdbRetainedStateV1>() as u32,
                ..HawdbRetainedStateV1::default()
            };
            match object.as_ref() {
                Object::Cursor(state) => {
                    let state = state.lock().map_err(|_| HAWDB_RETAINED_PANIC)?;
                    let profile = state.cursor.profile();
                    descriptor.status = state.cursor.status() as u32;
                    descriptor.terminal_code = state.terminal_code;
                    descriptor.visited_rows = profile.visited_rows as u64;
                    descriptor.emitted_rows = profile.emitted_rows as u64;
                    descriptor.source_constructed_bytes = profile.source_constructed_bytes as u64;
                    descriptor.source_pinned_rows = profile.source_pinned_rows as u64;
                    descriptor.source_pinned_pages = profile.source_pinned_pages as u64;
                }
                Object::Batch(batch) | Object::Column { batch, .. } => {
                    descriptor.status = batch.status() as u32
                }
            }
            Ok(descriptor)
        })
    }
}

/// Close a cursor and its source without revoking previously exported payload.
/// Release the closed cursor handle separately. Repeated close is harmless.
#[unsafe(no_mangle)]
pub extern "C" fn hawdb_retained_cursor_close(owner_namespace: u64, owner_id: u64) -> u32 {
    caught(|| {
        let object = lookup(owner_namespace, owner_id)?;
        let Object::Cursor(state) = object.as_ref() else {
            return Err(HAWDB_RETAINED_INVALID_HANDLE);
        };
        state
            .lock()
            .map_err(|_| HAWDB_RETAINED_PANIC)?
            .cursor
            .close();
        Ok(())
    })
}

/// Release exactly one owner. Stale, foreign-module and unknown handles return
/// InvalidHandle without dereferencing caller-provided addresses.
#[unsafe(no_mangle)]
pub extern "C" fn hawdb_retained_release(owner_namespace: u64, owner_id: u64) -> u32 {
    caught(|| {
        if owner_namespace != namespace() || owner_id == 0 {
            return Err(HAWDB_RETAINED_INVALID_HANDLE);
        }
        let object = REGISTRY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(owner_id)?;
        // Drop payload and charges outside the registry lock.
        if let Object::Cursor(state) = object.as_ref() {
            state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .cursor
                .close();
        }
        drop(object);
        Ok(())
    })
}

#[cfg(test)]
mod tests;
