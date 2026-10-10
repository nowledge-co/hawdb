// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Standard single-consumption Arrow capsules; descriptors, not result payload,
//! are allocated here. Native release owners keep the exact extension module.

use super::*;
use hawdb::{
    ArrowArray, ArrowArrayStream, ArrowSchema, RetainedArrowCodeOwner, RetainedArrowStream,
};
use pyo3::types::PyCapsule;

trait Descriptor: Sized {
    const NAME: &'static CStr;
    unsafe fn release(&mut self);
}
impl Descriptor for ArrowSchema {
    const NAME: &'static CStr = c"arrow_schema";
    unsafe fn release(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}
impl Descriptor for ArrowArray {
    const NAME: &'static CStr = c"arrow_array";
    unsafe fn release(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}
impl Descriptor for ArrowArrayStream {
    const NAME: &'static CStr = c"arrow_array_stream";
    unsafe fn release(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}

unsafe extern "C" fn destroy<T: Descriptor>(capsule: *mut ffi::PyObject) {
    // Preserve a pending Python exception while running a GC/release callback.
    let mut exception_type = ptr::null_mut();
    let mut exception_value = ptr::null_mut();
    let mut traceback = ptr::null_mut();
    unsafe { ffi::PyErr_Fetch(&mut exception_type, &mut exception_value, &mut traceback) };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // Consumers may rename a used capsule, but its original descriptor box
        // still belongs to this destructor. A move sets release to null.
        let name = unsafe { ffi::PyCapsule_GetName(capsule) };
        let pointer = unsafe { ffi::PyCapsule_GetPointer(capsule, name) };
        if !pointer.is_null() {
            let mut descriptor = unsafe { Box::from_raw(pointer.cast::<T>()) };
            unsafe { descriptor.release() };
        }
    }));
    unsafe {
        ffi::PyErr_Clear();
        ffi::PyErr_Restore(exception_type, exception_value, traceback);
    }
}
fn capsule<'py, T: Descriptor>(py: Python<'py>, descriptor: T) -> PyResult<Bound<'py, PyAny>> {
    let mut descriptor = Box::new(descriptor);
    let object = unsafe {
        ffi::PyCapsule_New(
            std::ptr::from_mut(descriptor.as_mut()).cast(),
            T::NAME.as_ptr(),
            Some(destroy::<T>),
        )
    };
    if object.is_null() {
        let error = PyErr::fetch(py);
        unsafe { descriptor.release() };
        return Err(error);
    }
    let _ = Box::into_raw(descriptor);
    Ok(unsafe { Bound::from_owned_ptr(py, object) })
}
fn wrapper<T: Descriptor>(py: Python<'_>) -> PyResult<usize> {
    std::mem::size_of::<T>()
        .checked_add(metadata::<PyCapsule>(py)?)
        .and_then(|n| n.checked_add(128))
        .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))
}
fn code(module: &Option<Arc<Py<PyAny>>>) -> Option<Arc<dyn RetainedArrowCodeOwner>> {
    module
        .as_ref()
        .map(|module| Arc::clone(module) as Arc<dyn RetainedArrowCodeOwner>)
}
fn request(py: Python<'_>, requested: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    if requested.is_some() {
        return Err(error(py, &RetainedQueryError::CopyRequired));
    }
    Ok(())
}
pub(super) fn cursor_schema<'py>(
    py: Python<'py>,
    cursor: &RetainedCursor,
) -> PyResult<Bound<'py, PyAny>> {
    let state = cursor
        .state
        .lock_py_attached(py)
        .unwrap_or_else(|p| p.into_inner());
    let cursor_native = state.inner.as_ref().ok_or_else(|| closed(py))?;
    let schema = cursor_native
        .export_arrow_schema_with_code_owner(wrapper::<ArrowSchema>(py)?, code(&cursor.module))
        .map_err(|source| error(py, &source))?;
    capsule(py, schema.into_raw())
}
pub(super) fn cursor_stream<'py>(
    py: Python<'py>,
    cursor: &RetainedCursor,
    requested: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    request(py, requested)?;
    let mut state = cursor
        .state
        .lock_py_attached(py)
        .unwrap_or_else(|p| p.into_inner());
    let stream = RetainedArrowStream::try_take_cursor(
        &mut state.inner,
        wrapper::<ArrowArrayStream>(py)?,
        code(&cursor.module),
    )
    .map_err(|source| error(py, &source))?;
    // Keep the owned stream until Python accepts its capsule. A failed Python
    // allocation returns the unchanged cursor instead of silently closing it.
    // The transparent wrapper has the standard descriptor's allocation layout.
    let stream = Box::new(stream);
    let descriptor = std::ptr::from_ref(stream.descriptor()).cast_mut();
    let object = unsafe {
        ffi::PyCapsule_New(
            descriptor.cast(),
            c"arrow_array_stream".as_ptr(),
            Some(destroy::<ArrowArrayStream>),
        )
    };
    if object.is_null() {
        let error = PyErr::fetch(py);
        state.inner = Some(stream.into_cursor());
        return Err(error);
    }
    let _ = Box::into_raw(stream);
    Ok(unsafe { Bound::from_owned_ptr(py, object) })
}
pub(super) fn batch_schema<'py>(
    py: Python<'py>,
    batch: &RetainedBatch,
) -> PyResult<Bound<'py, PyAny>> {
    let schema = batch
        .required(py)?
        .export_arrow_schema_with_code_owner(wrapper::<ArrowSchema>(py)?, code(&batch.module))
        .map_err(|source| error(py, &source))?;
    capsule(py, schema.into_raw())
}
pub(super) fn batch_array<'py>(
    py: Python<'py>,
    batch: &RetainedBatch,
    requested: Option<&Bound<'py, PyAny>>,
) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
    request(py, requested)?;
    let export = batch
        .required(py)?
        .export_arrow_with_code_owner(wrapper::<ArrowArray>(py)?, code(&batch.module))
        .map_err(|source| error(py, &source))?;
    let (schema, array) = export.into_raw();
    // If either capsule allocation fails, release the other raw descriptor or
    // completed capsule; successful ownership is never released twice.
    let schema = match capsule(py, schema) {
        Ok(schema) => schema,
        Err(error) => {
            let mut array = array;
            unsafe { Descriptor::release(&mut array) };
            return Err(error);
        }
    };
    let array = capsule(py, array)?;
    Ok((schema, array))
}
