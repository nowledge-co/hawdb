// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Experimental read-only numeric views; complete source/RSS qualification and
//! Arrow export remain open. Only buffer descriptors are built at this boundary.

use crate::errors::{BackpressureError, RetainedError};
use hawdb::{
    RetainedBufferProvenance, RetainedColumnRole, RetainedColumnType, RetainedColumnValues,
    RetainedQueryBatch, RetainedQueryCursor, RetainedQueryError, RetainedQueryOptions,
    RetainedQueryStatus, RetainedValidityView,
};
use pyo3::class::gc::{PyTraverseError, PyVisit};
use pyo3::exceptions::{PyBufferError, PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyDict, PyMemoryView};
use pyo3::{ffi, PyTypeInfo};
use std::ffi::{c_int, c_void, CStr};
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Arc, Mutex};

mod arrow;

pub(crate) fn error(py: Python<'_>, source: &RetainedQueryError) -> PyErr {
    let retryable = source.is_retryable();
    let kind = if retryable {
        "backpressure"
    } else {
        match source {
            RetainedQueryError::UnsupportedPlan => "unsupported_plan",
            RetainedQueryError::UnsupportedLayout => "unsupported_layout",
            RetainedQueryError::UnsupportedType => "unsupported_type",
            RetainedQueryError::CopyRequired => "copy_required",
            RetainedQueryError::SelectionRequiresMaterialization => {
                "selection_requires_materialization"
            }
            RetainedQueryError::WorkingUnitTooLarge => "working_unit_too_large",
            RetainedQueryError::ResultBudget { .. } => "result_budget",
            RetainedQueryError::Admission(_) | RetainedQueryError::RetainedAdmission(_) => {
                "admission"
            }
            RetainedQueryError::Execution(_) => "execution",
            RetainedQueryError::Stopped(_) => "stopped",
            RetainedQueryError::Closed => "closed",
            RetainedQueryError::InvalidColumn => "invalid_column",
            RetainedQueryError::SizeOverflow => "size_overflow",
            RetainedQueryError::AdapterDelivery => "adapter_delivery",
            RetainedQueryError::Backpressure { .. } => "backpressure",
        }
    };
    let exception = if retryable {
        BackpressureError::new_err(source.to_string())
    } else {
        RetainedError::new_err(source.to_string())
    };
    if let Err(error) = exception
        .value(py)
        .setattr("kind", kind)
        .and_then(|()| exception.value(py).setattr("retryable", retryable))
    {
        return error;
    }
    exception
}

fn closed(py: Python<'_>) -> PyErr {
    error(py, &RetainedQueryError::Closed)
}

fn metadata<T: PyTypeInfo>(py: Python<'_>) -> PyResult<usize> {
    // Actual Python object layout plus conservative GC/allocator/cell padding.
    T::type_object(py)
        .getattr("__basicsize__")?
        .extract::<usize>()?
        .checked_add(256)
        .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))
}

pub(crate) fn options(
    py: Python<'_>,
    rows: Option<usize>,
    bytes: Option<usize>,
    slots: Option<usize>,
    source_reuse: bool,
    writable: bool,
) -> PyResult<RetainedQueryOptions> {
    if writable {
        return Err(error(py, &RetainedQueryError::CopyRequired));
    }
    let nonzero = |value| {
        NonZeroUsize::new(value)
            .ok_or_else(|| PyValueError::new_err("retained limits must be positive"))
    };
    let mut options = RetainedQueryOptions::default();
    if let Some(value) = rows {
        options.batch_rows = nonzero(value)?;
    }
    if let Some(value) = bytes {
        options.batch_bytes = nonzero(value)?;
    }
    if let Some(value) = slots {
        options.outstanding_batches = nonzero(value)?;
    }
    options.require_source_reuse = source_reuse;
    options.adapter_metadata_bytes = metadata::<RetainedCursor>(py)?;
    // Control, batch, exporter and independently retained Py_buffer lease.
    options.minimum_shared_handles = NonZeroUsize::new(4).expect("nonzero protocol minimum");
    Ok(options)
}

fn status(state: RetainedQueryStatus) -> &'static str {
    match state {
        RetainedQueryStatus::Open => "open",
        RetainedQueryStatus::Completed => "completed",
        RetainedQueryStatus::Failed => "failed",
        RetainedQueryStatus::Closed => "closed",
    }
}

#[pyclass(module = "hawdb", frozen)]
pub struct RetainedOptions {
    pub(crate) inner: RetainedQueryOptions,
}
#[pymethods]
impl RetainedOptions {
    #[new]
    #[pyo3(signature = (*, batch_rows = None, batch_bytes = None, outstanding_batches = None, require_source_reuse = false, writable = false, max_result_rows = None))]
    fn new(
        py: Python<'_>,
        batch_rows: Option<usize>,
        batch_bytes: Option<usize>,
        outstanding_batches: Option<usize>,
        require_source_reuse: bool,
        writable: bool,
        max_result_rows: Option<usize>,
    ) -> PyResult<Self> {
        let mut inner = options(
            py,
            batch_rows,
            batch_bytes,
            outstanding_batches,
            require_source_reuse,
            writable,
        )?;
        inner.max_result_rows = max_result_rows;
        Ok(Self { inner })
    }
}

#[pyclass(module = "hawdb")]
pub struct RetainedCursor {
    state: Mutex<CursorState>,
    module: Option<Arc<Py<PyAny>>>,
}
struct CursorState {
    inner: Option<RetainedQueryCursor>,
    delivered_rows: usize,
    delivered_batches: usize,
}
fn clone_module(py: Python<'_>, module: &Option<Arc<Py<PyAny>>>) -> Option<Arc<Py<PyAny>>> {
    // Each Python owner contributes its own refcount/GC edge. Only native
    // descriptor owners share this Arc with that particular Python owner.
    module.as_ref().map(|module| Arc::new(module.clone_ref(py)))
}
fn visit_module(
    module: &Option<Arc<Py<PyAny>>>,
    visit: PyVisit<'_>,
) -> Result<(), PyTraverseError> {
    if let Some(module) = module
        && Arc::strong_count(module) == 1
    {
        visit.call(module.as_ref())?;
    }
    // A shared native owner is an external GC root, not another Python edge.
    Ok(())
}
impl RetainedCursor {
    pub(crate) fn new(inner: RetainedQueryCursor, module: Py<PyAny>) -> Self {
        Self {
            state: Mutex::new(CursorState {
                inner: Some(inner),
                delivered_rows: 0,
                delivered_batches: 0,
            }),
            module: Some(Arc::new(module)),
        }
    }
}
#[pymethods]
impl RetainedCursor {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        arrow::cursor_schema(py, self)
    }
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        arrow::cursor_stream(py, self, requested_schema)
    }
    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<Py<RetainedBatch>>> {
        let metadata = metadata::<RetainedBatch>(py)?;
        // Detach while waiting for the lock, then keep it through Python
        // delivery so close/adoption cannot hide a failed batch allocation.
        let mut state = self
            .state
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner());
        let cursor = state.inner.as_mut().ok_or_else(|| closed(py))?;
        let native = match catch_unwind(AssertUnwindSafe(|| {
            py.detach(|| cursor.next_batch_with_metadata(metadata))
        })) {
            Ok(result) => result.map_err(|source| error(py, &source))?,
            Err(_) => {
                cursor.abort_delivery();
                return Err(error(py, &RetainedQueryError::AdapterDelivery));
            }
        };
        let Some(native) = native else {
            return Ok(None);
        };
        let rows = native.selected_rows().len();
        match Py::new(
            py,
            RetainedBatch {
                inner: Some(native),
                module: clone_module(py, &self.module),
            },
        ) {
            Ok(batch) => {
                state.delivered_rows += rows;
                state.delivered_batches += 1;
                Ok(Some(batch))
            }
            Err(error) => {
                cursor.abort_delivery();
                Err(error)
            }
        }
    }
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<RetainedBatch>>> {
        self.next_batch(py)
    }
    fn close(&self, py: Python<'_>) {
        self.state
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner())
            .inner
            .take();
    }
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit_module(&self.module, visit)
    }
    fn __clear__(&mut self) {
        self.state
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .inner
            .take();
        self.module = None;
    }
    #[getter]
    fn status(&self, py: Python<'_>) -> &'static str {
        self.state
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner())
            .inner
            .as_ref()
            .map_or("closed", |cursor| status(cursor.status()))
    }
    #[getter]
    fn column_count(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self
            .state
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner())
            .inner
            .as_ref()
            .ok_or_else(|| closed(py))?
            .schema()
            .len())
    }
    fn schema_copy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        let state = self
            .state
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner());
        let cursor = state.inner.as_ref().ok_or_else(|| closed(py))?;
        let result = pyo3::types::PyList::empty(py);
        for column in cursor.schema() {
            let item = PyDict::new(py);
            item.set_item("name", &column.name)?;
            item.set_item(
                "format",
                format(column.data_type).to_str().expect("ASCII format"),
            )?;
            item.set_item("nullable", column.nullable)?;
            item.set_item(
                "node_identity",
                column.role == RetainedColumnRole::NodeIdentity,
            )?;
            result.append(item)?;
        }
        Ok(result)
    }
    fn profile_copy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let state = self
            .state
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner());
        let cursor = state.inner.as_ref().ok_or_else(|| closed(py))?;
        let profile = cursor.profile();
        let result = PyDict::new(py);
        result.set_item("status", status(cursor.status()))?;
        for (name, value) in [
            ("visited_rows", profile.visited_rows),
            ("native_emitted_rows", profile.emitted_rows),
            ("source_constructed_bytes", profile.source_constructed_bytes),
            ("source_pinned_rows", profile.source_pinned_rows),
            ("source_pinned_pages", profile.source_pinned_pages),
            ("outstanding_batches", cursor.outstanding_batches()),
            ("delivered_rows", state.delivered_rows),
            ("delivered_batches", state.delivered_batches),
        ] {
            result.set_item(name, value)?;
        }
        Ok(result)
    }
}

#[pyclass(module = "hawdb")]
pub struct RetainedBatch {
    inner: Option<RetainedQueryBatch>,
    module: Option<Arc<Py<PyAny>>>,
}
impl RetainedBatch {
    fn required(&self, py: Python<'_>) -> PyResult<&RetainedQueryBatch> {
        self.inner.as_ref().ok_or_else(|| closed(py))
    }
}
#[pymethods]
impl RetainedBatch {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        arrow::batch_schema(py, self)
    }
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        arrow::batch_array(py, self, requested_schema)
    }
    fn close(&mut self) {
        self.inner.take();
    }
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit_module(&self.module, visit)
    }
    fn __clear__(&mut self) {
        self.close();
        self.module = None;
    }
    #[getter]
    fn row_count(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.required(py)?.selected_rows().len())
    }
    #[getter]
    fn physical_rows(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.required(py)?.physical_rows())
    }
    #[getter]
    fn column_count(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.required(py)?.schema().len())
    }
    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<&'static str> {
        Ok(status(self.required(py)?.status()))
    }
    fn retain(&self, py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            inner: Some(
                self.required(py)?
                    .try_retain(metadata::<Self>(py)?)
                    .map_err(|source| error(py, &source))?,
            ),
            module: clone_module(py, &self.module),
        })
    }
    #[pyo3(signature = (index, *, writable = false, dtype = None))]
    fn column(
        &self,
        py: Python<'_>,
        index: usize,
        writable: bool,
        dtype: Option<&str>,
    ) -> PyResult<RetainedBuffer> {
        let batch = self.required(py)?;
        let schema = batch
            .schema()
            .get(index)
            .ok_or_else(|| error(py, &RetainedQueryError::InvalidColumn))?;
        if writable
            || dtype.is_some_and(|dtype| {
                dtype != format(schema.data_type).to_str().expect("ASCII format")
            })
        {
            return Err(error(py, &RetainedQueryError::CopyRequired));
        }
        RetainedBuffer::new(py, batch, BufferKind::Values(index), &self.module)
    }
    fn selection(&self, py: Python<'_>) -> PyResult<RetainedBuffer> {
        RetainedBuffer::new(py, self.required(py)?, BufferKind::Selection, &self.module)
    }
    /// Explicit scalar object materialization through the held batch, with no
    /// additional owner. This is available even at the shared handle limit.
    fn value_copy(&self, py: Python<'_>, column: usize, row: usize) -> PyResult<Py<PyAny>> {
        let batch = self.required(py)?;
        let physical = *batch
            .selected_rows()
            .get(row)
            .ok_or_else(|| PyIndexError::new_err("result row out of range"))?
            as usize;
        let values = batch.column(column).map_err(|source| error(py, &source))?;
        let valid = match batch
            .validity(column)
            .map_err(|source| error(py, &source))?
        {
            RetainedValidityView::All { .. } => true,
            RetainedValidityView::Bitmap { words, .. } => {
                words[physical / 64] & (1 << (physical % 64)) != 0
            }
        };
        if !valid {
            return Ok(py.None());
        }
        Ok(match values {
            RetainedColumnValues::Int64(values) => {
                values[physical].into_pyobject(py)?.into_any().unbind()
            }
            RetainedColumnValues::Float64(values) => {
                values[physical].into_pyobject(py)?.into_any().unbind()
            }
            RetainedColumnValues::UInt64(values) => {
                values[physical].into_pyobject(py)?.into_any().unbind()
            }
        })
    }
}

#[derive(Clone, Copy)]
enum BufferKind {
    Values(usize),
    Selection,
    Validity(usize),
}

#[pyclass(module = "hawdb")]
pub struct RetainedBuffer {
    inner: Option<RetainedQueryBatch>,
    kind: BufferKind,
    // A native exporter/lease keeps the exact code module, never a database.
    module: Option<Arc<Py<PyAny>>>,
}
struct BufferSpec {
    data: *mut c_void,
    format: &'static CStr,
    itemsize: usize,
    elements: usize,
    provenance: RetainedBufferProvenance,
}
struct BufferLease {
    _owner: RetainedQueryBatch,
    shape: isize,
    stride: isize,
}
fn format(kind: RetainedColumnType) -> &'static CStr {
    match kind {
        RetainedColumnType::Int64 => c"q",
        RetainedColumnType::Float64 => c"d",
        RetainedColumnType::UInt64 => c"Q",
    }
}
fn spec(batch: &RetainedQueryBatch, kind: BufferKind) -> Result<BufferSpec, RetainedQueryError> {
    let (data, format, itemsize, elements, provenance) = match kind {
        BufferKind::Values(index) => {
            let (data, elements) = match batch.column(index)? {
                RetainedColumnValues::Int64(values) => {
                    (values.as_ptr().cast_mut().cast(), values.len())
                }
                RetainedColumnValues::Float64(values) => {
                    (values.as_ptr().cast_mut().cast(), values.len())
                }
                RetainedColumnValues::UInt64(values) => {
                    (values.as_ptr().cast_mut().cast(), values.len())
                }
            };
            (
                data,
                format(batch.schema()[index].data_type),
                8,
                elements,
                batch.column_provenance(index)?,
            )
        }
        BufferKind::Selection => (
            batch.selected_rows().as_ptr().cast_mut().cast(),
            c"I",
            4,
            batch.selected_rows().len(),
            batch.selection_provenance(),
        ),
        BufferKind::Validity(index) => {
            let RetainedValidityView::Bitmap { words, .. } = batch.validity(index)? else {
                return Err(RetainedQueryError::UnsupportedLayout);
            };
            (
                words.as_ptr().cast_mut().cast(),
                c"Q",
                8,
                words.len(),
                batch
                    .validity_provenance(index)?
                    .ok_or(RetainedQueryError::UnsupportedLayout)?,
            )
        }
    };
    Ok(BufferSpec {
        data,
        format,
        itemsize,
        elements,
        provenance,
    })
}
impl RetainedBuffer {
    fn new(
        py: Python<'_>,
        batch: &RetainedQueryBatch,
        kind: BufferKind,
        module: &Option<Arc<Py<PyAny>>>,
    ) -> PyResult<Self> {
        spec(batch, kind).map_err(|source| error(py, &source))?;
        let inner = batch
            .try_retain(metadata::<Self>(py)?)
            .map_err(|source| error(py, &source))?;
        Ok(Self {
            inner: Some(inner),
            kind,
            module: clone_module(py, module),
        })
    }
    fn required(&self, py: Python<'_>) -> PyResult<&RetainedQueryBatch> {
        self.inner.as_ref().ok_or_else(|| closed(py))
    }
}
#[pymethods]
impl RetainedBuffer {
    fn close(&mut self) {
        self.inner.take();
    }
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit_module(&self.module, visit)
    }
    fn __clear__(&mut self) {
        self.close();
        self.module = None;
    }
    fn retain(&self, py: Python<'_>) -> PyResult<Self> {
        Self::new(py, self.required(py)?, self.kind, &self.module)
    }
    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<&'static str> {
        Ok(status(self.required(py)?.status()))
    }
    #[getter]
    fn physical_rows(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.required(py)?.physical_rows())
    }
    #[getter]
    fn row_count(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.required(py)?.selected_rows().len())
    }
    fn selection(&self, py: Python<'_>) -> PyResult<Self> {
        Self::new(py, self.required(py)?, BufferKind::Selection, &self.module)
    }
    fn validity(&self, py: Python<'_>) -> PyResult<Option<Self>> {
        let batch = self.required(py)?;
        let BufferKind::Values(index) = self.kind else {
            return Err(error(py, &RetainedQueryError::UnsupportedLayout));
        };
        match batch.validity(index).map_err(|source| error(py, &source))? {
            RetainedValidityView::All { .. } => Ok(None),
            RetainedValidityView::Bitmap { .. } => {
                Self::new(py, batch, BufferKind::Validity(index), &self.module).map(Some)
            }
        }
    }
    fn provenance_copy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let spec = spec(self.required(py)?, self.kind).map_err(|source| error(py, &source))?;
        let view = spec.provenance;
        let result = PyDict::new(py);
        result.set_item("namespace", view.identity.namespace)?;
        result.set_item("allocation", view.identity.allocation)?;
        result.set_item("generation", view.identity.generation)?;
        result.set_item("capacity_bytes", view.retained_capacity_bytes)?;
        result.set_item("offset_bytes", view.byte_offset)?;
        result.set_item("length_bytes", view.byte_length)?;
        Ok(result)
    }
    /// # Safety
    /// CPython supplies an aligned writable Py_buffer or null. Pointed-to
    /// arrays are retained in internal until the matching release callback.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        if view.is_null() {
            return Err(PyBufferError::new_err("null buffer output"));
        }
        unsafe { view.write(ffi::Py_buffer::new()) };
        if flags & ffi::PyBUF_WRITABLE != 0 {
            return Err(PyBufferError::new_err("retained buffers are read-only"));
        }
        let allowed = ffi::PyBUF_FULL_RO
            | ffi::PyBUF_C_CONTIGUOUS
            | ffi::PyBUF_F_CONTIGUOUS
            | ffi::PyBUF_ANY_CONTIGUOUS;
        if flags & !allowed != 0 {
            return Err(PyBufferError::new_err("unsupported buffer request flags"));
        }
        if flags & (ffi::PyBUF_FORMAT | 0x10) != 0 && flags & ffi::PyBUF_ND == 0
            || flags & 0x1e0 != 0 && flags & ffi::PyBUF_STRIDES != ffi::PyBUF_STRIDES
        {
            return Err(PyBufferError::new_err("inconsistent buffer request flags"));
        }
        let exporter = slf.try_borrow()?;
        let batch = exporter.required(py)?;
        let spec = spec(batch, exporter.kind).map_err(|source| error(py, &source))?;
        let bytes = spec
            .elements
            .checked_mul(spec.itemsize)
            .and_then(|bytes| isize::try_from(bytes).ok())
            .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))?;
        let shape = isize::try_from(spec.elements)
            .map_err(|_| error(py, &RetainedQueryError::SizeOverflow))?;
        // The native descriptor/shape owner and opaque one-dimensional
        // memoryview/managed-buffer overhead are admitted before Box allocation.
        let metadata = std::mem::size_of::<BufferLease>()
            + std::mem::size_of::<ffi::Py_buffer>()
            + metadata::<PyMemoryView>(py)?
            + 512;
        let owner = batch
            .try_retain(metadata)
            .map_err(|source| error(py, &source))?;
        let mut lease = Box::new(BufferLease {
            _owner: owner,
            shape,
            stride: spec.itemsize as isize,
        });
        let mut descriptor = ffi::Py_buffer::new();
        descriptor.buf = spec.data;
        descriptor.len = bytes;
        descriptor.readonly = 1;
        descriptor.itemsize = spec.itemsize as isize;
        descriptor.ndim = 1;
        if flags & ffi::PyBUF_FORMAT != 0 {
            descriptor.format = spec.format.as_ptr().cast_mut();
        }
        if flags & ffi::PyBUF_ND != 0 {
            descriptor.shape = &mut lease.shape;
        }
        if flags & ffi::PyBUF_STRIDES == ffi::PyBUF_STRIDES {
            descriptor.strides = &mut lease.stride;
        }
        drop(exporter);
        descriptor.obj = slf.into_any().into_ptr();
        descriptor.internal = Box::into_raw(lease).cast();
        unsafe { view.write(descriptor) };
        Ok(())
    }
    /// # Safety
    /// view comes from this exporter's successful getbuffer callback. Release
    /// exactly its internal lease, leaving obj for PyBuffer_Release to decref.
    unsafe fn __releasebuffer__(_slf: Bound<'_, Self>, view: *mut ffi::Py_buffer) {
        if !view.is_null() {
            let internal = unsafe { (*view).internal };
            if !internal.is_null() {
                unsafe {
                    (*view).internal = ptr::null_mut();
                    drop(Box::from_raw(internal.cast::<BufferLease>()));
                }
            }
        }
    }
}
