// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Experimental Arrow C Data descriptors for compatible numeric selections.
//! Payload stays in the native batch. Descriptor release follows Arrow's move
//! rules, including independently moved children; no Arrow library is required.

use super::*;
use std::ffi::{c_char, c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

mod stream;
pub use stream::{ArrowArrayStream, RetainedArrowStream};

/// Foreign code lifetime shared by every descriptor, including moved children.
/// The owner must keep release code usable throughout the final callback.
pub trait RetainedArrowCodeOwner: std::fmt::Debug + Send + Sync {}
impl<T: std::fmt::Debug + Send + Sync> RetainedArrowCodeOwner for T {}
type CodeOwner = Option<Arc<dyn RetainedArrowCodeOwner>>;

/// Standard Arrow C Data schema. Raw consumers must obey Arrow's move/release
/// contract. Do not copy a live descriptor without marking its source released.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

impl Default for ArrowSchema {
    fn default() -> Self {
        Self {
            format: ptr::null(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

/// Standard Arrow C Data array; its immutable buffers remain valid until the
/// single active descriptor is released, even after cursor/database closure.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}
impl Default for ArrowArray {
    fn default() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: ptr::null_mut(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

/// Rust owner for an independently releasable Arrow schema. Schema ownership
/// retains admitted schema metadata, without keeping a result slot or payload.
#[derive(Debug)]
pub struct RetainedArrowSchema(ArrowSchema);
impl RetainedArrowSchema {
    pub fn descriptor(&self) -> &ArrowSchema {
        &self.0
    }
    /// Transfers the release obligation to an Arrow C Data consumer.
    pub fn into_raw(mut self) -> ArrowSchema {
        std::mem::take(&mut self.0)
    }
}
impl Drop for RetainedArrowSchema {
    fn drop(&mut self) {
        if let Some(release) = self.0.release {
            unsafe { release(&mut self.0) };
        }
    }
}

/// Rust owner for a record batch's Arrow C Data schema and array. Only metadata
/// is constructed; values and validity share their original native allocations.
#[derive(Debug)]
pub struct RetainedArrowExport {
    schema: RetainedArrowSchema,
    array: ArrowArray,
    batch: Arc<RetainedQueryBatch>,
}
impl RetainedArrowExport {
    pub fn schema(&self) -> &ArrowSchema {
        self.schema.descriptor()
    }
    pub fn array(&self) -> &ArrowArray {
        &self.array
    }
    /// Completion/error remains observable while the Rust export is retained.
    pub fn status(&self) -> RetainedQueryStatus {
        self.batch.status()
    }
    /// Allocation identity and selected value range, without allocating metadata.
    pub fn column_provenance(&self, index: usize) -> Result<NumericBufferProvenance> {
        let mut view = self.batch.column_provenance(index)?;
        let (offset, length) = range(&self.batch)?;
        let offset = usize::try_from(offset)
            .ok()
            .and_then(|n| n.checked_mul(8))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        view.byte_offset = view
            .byte_offset
            .checked_add(offset)
            .ok_or(RetainedQueryError::SizeOverflow)?;
        view.byte_length = usize::try_from(length)
            .ok()
            .and_then(|n| n.checked_mul(8))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        Ok(view)
    }
    /// Transfers both release obligations to an Arrow C Data consumer.
    pub fn into_raw(mut self) -> (ArrowSchema, ArrowArray) {
        let schema = std::mem::take(&mut self.schema.0);
        (schema, std::mem::take(&mut self.array))
    }
}
impl Drop for RetainedArrowExport {
    fn drop(&mut self) {
        if let Some(release) = self.array.release {
            unsafe { release(&mut self.array) };
        }
    }
}

#[derive(Debug)]
struct MetadataLease {
    _shared: Arc<CursorShared>,
    _code: CodeOwner,
    _memory: QueryMemoryLease,
    _runtime: RuntimeRetainedResult,
}
fn admit(shared: &Arc<CursorShared>, bytes: usize, code: &CodeOwner) -> Result<MetadataLease> {
    let runtime = shared._runtime.try_retain(bytes as u64)?;
    let memory = shared.account.reserve(
        usize::try_from(runtime.handle_bytes()).map_err(|_| RetainedQueryError::SizeOverflow)?,
    )?;
    Ok(MetadataLease {
        _shared: Arc::clone(shared),
        _code: code.clone(),
        _memory: memory,
        _runtime: runtime,
    })
}
fn capacity<T>(extra: usize) -> Result<usize> {
    std::mem::size_of::<T>()
        .checked_add(extra)
        .and_then(|n| n.checked_add(64))
        .ok_or(RetainedQueryError::SizeOverflow)
}
fn handles(shared: &CursorShared, count: usize) -> Result<()> {
    if count > shared.handle_limit {
        return Err(RetainedQueryError::WorkingUnitTooLarge);
    }
    Ok(())
}

struct SchemaOwner {
    name: Option<CString>,
    metadata: Vec<u8>,
    children: Vec<ArrowSchema>,
    pointers: Vec<*mut ArrowSchema>,
    _lease: MetadataLease,
}
impl Drop for SchemaOwner {
    fn drop(&mut self) {
        for child in &mut self.children {
            if let Some(release) = child.release {
                unsafe { release(child) };
            }
        }
    }
}
unsafe extern "C" fn release_schema(schema: *mut ArrowSchema) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(schema) = unsafe { schema.as_mut() }
            && schema.release.take().is_some()
        {
            let owner = std::mem::replace(&mut schema.private_data, ptr::null_mut());
            unsafe {
                drop(Box::from_raw(owner.cast::<SchemaOwner>()));
            }
        }
    }));
}

fn schema(
    shared: &Arc<CursorShared>,
    wrapper_bytes: usize,
    code: &CodeOwner,
) -> Result<RetainedArrowSchema> {
    if shared
        .schema
        .iter()
        .any(|column| column.name.as_bytes().contains(&0))
    {
        return Err(RetainedQueryError::UnsupportedLayout);
    }
    let count = shared.schema.len();
    handles(
        shared,
        count
            .checked_add(2)
            .ok_or(RetainedQueryError::SizeOverflow)?,
    )?;
    let parent_bytes = count
        .checked_mul(std::mem::size_of::<ArrowSchema>() + std::mem::size_of::<*mut ArrowSchema>())
        .and_then(|n| n.checked_add(wrapper_bytes))
        .and_then(|n| n.checked_add(std::mem::size_of::<ArrowSchema>()))
        .ok_or(RetainedQueryError::SizeOverflow)?;
    let lease = admit(shared, capacity::<SchemaOwner>(parent_bytes)?, code)?;
    let mut parent = Box::new(SchemaOwner {
        name: None,
        metadata: Vec::new(),
        children: Vec::with_capacity(count),
        pointers: Vec::with_capacity(count),
        _lease: lease,
    });
    for column in &shared.schema {
        let identity = column.role == RetainedColumnRole::NodeIdentity;
        let name_bytes = column
            .name
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_add(35 * usize::from(identity)))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let lease = admit(shared, capacity::<SchemaOwner>(name_bytes)?, code)?;
        let mut owner = Box::new(SchemaOwner {
            name: Some(
                CString::new(column.name.as_bytes())
                    .map_err(|_| RetainedQueryError::UnsupportedLayout)?,
            ),
            metadata: Vec::with_capacity(35 * usize::from(identity)),
            children: Vec::new(),
            pointers: Vec::new(),
            _lease: lease,
        });
        if identity {
            // Standard native-endian Arrow metadata; preserves the identity role.
            owner.metadata.extend_from_slice(&1i32.to_ne_bytes());
            owner.metadata.extend_from_slice(&10i32.to_ne_bytes());
            owner.metadata.extend_from_slice(b"hawdb:role");
            owner.metadata.extend_from_slice(&13i32.to_ne_bytes());
            owner.metadata.extend_from_slice(b"node_identity");
        }
        let format = match column.data_type {
            RetainedColumnType::Int64 => c"l",
            RetainedColumnType::Float64 => c"g",
            RetainedColumnType::UInt64 => c"L",
        };
        let descriptor = ArrowSchema {
            format: format.as_ptr(),
            name: owner.name.as_ref().expect("name").as_ptr(),
            metadata: if identity {
                owner.metadata.as_ptr().cast()
            } else {
                ptr::null()
            },
            flags: if column.nullable { 2 } else { 0 },
            release: Some(release_schema),
            private_data: Box::into_raw(owner).cast(),
            ..ArrowSchema::default()
        };
        parent.children.push(descriptor);
    }
    parent
        .pointers
        .extend(parent.children.iter_mut().map(|child| child as *mut _));
    let descriptor = ArrowSchema {
        format: c"+s".as_ptr(),
        name: c"".as_ptr(),
        n_children: i64::try_from(count).map_err(|_| RetainedQueryError::SizeOverflow)?,
        children: parent.pointers.as_mut_ptr(),
        release: Some(release_schema),
        private_data: Box::into_raw(parent).cast(),
        ..ArrowSchema::default()
    };
    Ok(RetainedArrowSchema(descriptor))
}

struct ArrayOwner {
    buffers: [*const c_void; 2],
    children: Vec<ArrowArray>,
    pointers: Vec<*mut ArrowArray>,
    _batch: Option<Arc<RetainedQueryBatch>>,
    _lease: MetadataLease,
}
impl Drop for ArrayOwner {
    fn drop(&mut self) {
        for child in &mut self.children {
            if let Some(release) = child.release {
                unsafe { release(child) };
            }
        }
    }
}
unsafe extern "C" fn release_array(array: *mut ArrowArray) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(array) = unsafe { array.as_mut() }
            && array.release.take().is_some()
        {
            let owner = std::mem::replace(&mut array.private_data, ptr::null_mut());
            unsafe {
                drop(Box::from_raw(owner.cast::<ArrayOwner>()));
            }
        }
    }));
}

fn range(batch: &RetainedQueryBatch) -> Result<(i64, i64)> {
    let selected = batch.selected_rows();
    let first = selected.first().copied().unwrap_or(0);
    for (position, row) in selected.iter().enumerate() {
        if *row as usize >= batch.physical_rows()
            || first
                .checked_add(u32::try_from(position).map_err(|_| RetainedQueryError::SizeOverflow)?)
                != Some(*row)
        {
            return Err(RetainedQueryError::SelectionRequiresMaterialization);
        }
    }
    Ok((
        i64::from(first),
        i64::try_from(selected.len()).map_err(|_| RetainedQueryError::SizeOverflow)?,
    ))
}

// Admit every descriptor before a stream asks its native cursor for a batch.
// Temporary reservation-directory capacity stays conservatively charged in the
// parent lease, including while final descriptors are being constructed.
struct ArrayReservation {
    parent: MetadataLease,
    children: Vec<MetadataLease>,
}
impl ArrayReservation {
    fn new(shared: &Arc<CursorShared>, wrapper_bytes: usize, code: &CodeOwner) -> Result<Self> {
        let count = shared.schema.len();
        let extra = count
            .checked_mul(
                std::mem::size_of::<ArrowArray>()
                    + std::mem::size_of::<*mut ArrowArray>()
                    + std::mem::size_of::<MetadataLease>(),
            )
            .and_then(|n| n.checked_add(wrapper_bytes))
            .and_then(|n| n.checked_add(std::mem::size_of::<ArrowArray>()))
            .and_then(|n| n.checked_add(std::mem::size_of::<Arc<RetainedQueryBatch>>()))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let parent = admit(shared, capacity::<ArrayOwner>(extra)?, code)?;
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(admit(shared, capacity::<ArrayOwner>(0)?, code)?);
        }
        Ok(Self { parent, children })
    }
    fn build(self, batch: Arc<RetainedQueryBatch>) -> Result<ArrowArray> {
        let (offset, length) = range(&batch)?;
        let count = self.children.len();
        let mut parent = Box::new(ArrayOwner {
            buffers: [ptr::null(); 2],
            children: Vec::with_capacity(count),
            pointers: Vec::with_capacity(count),
            _batch: None,
            _lease: self.parent,
        });
        for (index, lease) in self.children.into_iter().enumerate() {
            let values = match batch.column(index)? {
                RetainedColumnValues::Int64(values) => values.as_ptr().cast(),
                RetainedColumnValues::Float64(values) => values.as_ptr().cast(),
                RetainedColumnValues::UInt64(values) => values.as_ptr().cast(),
            };
            let (validity, null_count) = match batch.validity(index)? {
                ValidityView::All { .. } => (ptr::null(), 0),
                ValidityView::Bitmap { words, .. } => {
                    (words.as_ptr().cast(), if length == 0 { 0 } else { -1 })
                }
            };
            let mut owner = Box::new(ArrayOwner {
                buffers: [validity, values],
                children: Vec::new(),
                pointers: Vec::new(),
                _batch: Some(Arc::clone(&batch)),
                _lease: lease,
            });
            let descriptor = ArrowArray {
                length,
                offset,
                null_count,
                n_buffers: 2,
                buffers: owner.buffers.as_mut_ptr(),
                release: Some(release_array),
                private_data: Box::into_raw(owner).cast(),
                ..ArrowArray::default()
            };
            parent.children.push(descriptor);
        }
        parent
            .pointers
            .extend(parent.children.iter_mut().map(|child| child as *mut _));
        Ok(ArrowArray {
            length,
            n_buffers: 1,
            n_children: i64::try_from(count).map_err(|_| RetainedQueryError::SizeOverflow)?,
            buffers: parent.buffers.as_mut_ptr(),
            children: parent.pointers.as_mut_ptr(),
            release: Some(release_array),
            private_data: Box::into_raw(parent).cast(),
            ..ArrowArray::default()
        })
    }
}

impl RetainedQueryBatch {
    /// Strict Arrow C Data export. Sparse/reordered selection refuses explicitly.
    /// Wrapper capacity is prepaid separately in schema and array descriptors.
    pub fn export_arrow(&self, wrapper_bytes: usize) -> Result<RetainedArrowExport> {
        self.export_arrow_with_code_owner(wrapper_bytes, None)
    }
    /// The supplied module/library owner follows every independently moved child.
    /// Its allocation belongs in the caller's prepaid wrapper capacity.
    pub fn export_arrow_with_code_owner(
        &self,
        wrapper_bytes: usize,
        code: Option<Arc<dyn RetainedArrowCodeOwner>>,
    ) -> Result<RetainedArrowExport> {
        range(self)?;
        let shared = &self.slot.shared;
        handles(
            shared,
            self.schema()
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(5))
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        let schema = schema(shared, wrapper_bytes, &code)?;
        let reservation = ArrayReservation::new(shared, wrapper_bytes, &code)?;
        let batch = Arc::new(self.try_retain(capacity::<Arc<RetainedQueryBatch>>(0)?)?);
        let array = reservation.build(Arc::clone(&batch))?;
        Ok(RetainedArrowExport {
            schema,
            array,
            batch,
        })
    }
    pub fn export_arrow_schema_with_code_owner(
        &self,
        wrapper_bytes: usize,
        code: Option<Arc<dyn RetainedArrowCodeOwner>>,
    ) -> Result<RetainedArrowSchema> {
        handles(
            &self.slot.shared,
            self.schema()
                .len()
                .checked_add(3)
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        schema(&self.slot.shared, wrapper_bytes, &code)
    }
    /// Independently admitted metadata export without retaining the result slot.
    pub fn export_arrow_schema(&self, wrapper_bytes: usize) -> Result<RetainedArrowSchema> {
        handles(
            &self.slot.shared,
            self.schema()
                .len()
                .checked_add(3)
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        schema(&self.slot.shared, wrapper_bytes, &None)
    }
}

impl RetainedQueryCursor {
    pub fn export_arrow_schema_with_code_owner(
        &self,
        wrapper_bytes: usize,
        code: Option<Arc<dyn RetainedArrowCodeOwner>>,
    ) -> Result<RetainedArrowSchema> {
        schema(&self.shared, wrapper_bytes, &code)
    }
    pub fn export_arrow_schema(&self, wrapper_bytes: usize) -> Result<RetainedArrowSchema> {
        schema(&self.shared, wrapper_bytes, &None)
    }
}
