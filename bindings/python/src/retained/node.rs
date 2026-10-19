// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Indexed row owners cross Python without constructing a node dictionary.
//! Nested containers expose child owners, not materialized Python containers.

use super::*;
use hawdb::Value;
use hawdb::{
    RetainedNodeCursor as NativeCursor, RetainedNodeField, RetainedNodeView as NativeView,
};

#[pyclass(module = "hawdb")]
pub struct RetainedNodeCursor {
    inner: Mutex<Option<NativeCursor>>,
    module: Option<Arc<Py<PyAny>>>,
}
impl RetainedNodeCursor {
    pub(crate) fn new(inner: NativeCursor, module: Py<PyAny>) -> Self {
        Self {
            inner: Mutex::new(Some(inner)),
            module: Some(Arc::new(module)),
        }
    }
}
#[pymethods]
impl RetainedNodeCursor {
    fn next_node(&self, py: Python<'_>) -> PyResult<Option<Py<RetainedNodeView>>> {
        let mut guard = self
            .inner
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner());
        let cursor = guard.as_mut().ok_or_else(|| closed(py))?;
        let native = match catch_unwind(AssertUnwindSafe(|| py.detach(|| cursor.next_node()))) {
            Ok(result) => result.map_err(|source| error(py, &source))?,
            Err(_) => {
                cursor.abort_delivery();
                return Err(error(py, &RetainedQueryError::AdapterDelivery));
            }
        };
        let Some(native) = native else {
            return Ok(None);
        };
        match Py::new(
            py,
            RetainedNodeView {
                location: Location::Node,
                inner: Some(native),
                module: clone_module(py, &self.module),
            },
        ) {
            Ok(view) => Ok(Some(view)),
            Err(error) => {
                cursor.abort_delivery();
                Err(error)
            }
        }
    }
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<RetainedNodeView>>> {
        self.next_node(py)
    }
    fn close(&self, py: Python<'_>) {
        self.inner
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner())
            .take();
    }
    #[getter]
    fn access_path(&self, py: Python<'_>) -> PyResult<&'static str> {
        Ok(self
            .inner
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .ok_or_else(|| closed(py))?
            .access_path())
    }
    #[getter]
    fn status(&self, py: Python<'_>) -> &'static str {
        self.inner
            .lock_py_attached(py)
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or("closed", |cursor| status(cursor.status()))
    }
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit_module(&self.module, visit)
    }
    fn __clear__(&mut self) {
        self.inner
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        self.module = None;
    }
}

#[derive(Clone)]
enum Location {
    Node,
    Identity,
    Labels,
    Label(usize),
    Value(Box<[usize]>),
}
enum Child {
    Identity,
    Labels,
    Label(usize),
    Value(usize),
}

#[pyclass(module = "hawdb")]
pub struct RetainedNodeView {
    // Descriptor paths are destroyed before their admitted native owner.
    location: Location,
    inner: Option<NativeView>,
    module: Option<Arc<Py<PyAny>>>,
}
impl RetainedNodeView {
    fn required(&self, py: Python<'_>) -> PyResult<&NativeView> {
        self.inner.as_ref().ok_or_else(|| closed(py))
    }
    fn value(&self, py: Python<'_>) -> PyResult<&Value> {
        let Location::Value(path) = &self.location else {
            return Err(error(py, &RetainedQueryError::UnsupportedType));
        };
        let owner = self.required(py)?;
        let mut value = owner
            .properties()
            .map_err(|source| error(py, &source))?
            .nth(path[0])
            .ok_or_else(|| error(py, &RetainedQueryError::InvalidColumn))?
            .1;
        for index in &path[1..] {
            value = match value {
                Value::List(values) => values.get(*index),
                Value::Map(values) => values.values().nth(*index),
                _ => None,
            }
            .ok_or_else(|| error(py, &RetainedQueryError::InvalidColumn))?;
        }
        Ok(value)
    }
    fn child(&self, py: Python<'_>, child: Child) -> PyResult<Self> {
        let length = match (&self.location, &child) {
            (Location::Value(path), Child::Value(_)) => path.len().checked_add(1),
            (_, Child::Value(_)) => Some(1),
            _ => Some(0),
        }
        .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))?;
        let object_bytes = metadata::<Self>(py)?;
        let bytes = length
            .checked_mul(std::mem::size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(object_bytes))
            .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))?;
        let inner = self
            .required(py)?
            .try_retain_with_metadata(bytes)
            .map_err(|source| error(py, &source))?;
        // Admission precedes the descriptor allocation; payload is never cloned.
        let location = match child {
            Child::Identity => Location::Identity,
            Child::Labels => Location::Labels,
            Child::Label(index) => Location::Label(index),
            Child::Value(index) => {
                let mut path = Vec::with_capacity(length);
                if let Location::Value(parent) = &self.location {
                    path.extend_from_slice(parent);
                }
                path.push(index);
                Location::Value(path.into_boxed_slice())
            }
        };
        Ok(Self {
            location,
            inner: Some(inner),
            module: clone_module(py, &self.module),
        })
    }
    fn buffer(&self, py: Python<'_>) -> PyResult<NodeBufferSpec> {
        self.required(py)?;
        if matches!(self.location, Location::Identity) {
            let identity = self
                .required(py)?
                .identity_storage()
                .map_err(|source| error(py, &source))?;
            return Ok(NodeBufferSpec::scalar(identity, c"q"));
        }
        let bytes = match self.location {
            Location::Label(index) => match self
                .required(py)?
                .field("labels")
                .map_err(|source| error(py, &source))?
            {
                Some(RetainedNodeField::Labels(labels)) => {
                    labels.iter().nth(index).map(str::as_bytes)
                }
                _ => None,
            }
            .ok_or_else(|| error(py, &RetainedQueryError::InvalidColumn))?,
            Location::Value(_) => match self.value(py)? {
                Value::Int(value) => return Ok(NodeBufferSpec::scalar(value, c"q")),
                Value::Float(value) => return Ok(NodeBufferSpec::scalar(value, c"d")),
                Value::Bool(value) => return Ok(NodeBufferSpec::scalar(value, c"?")),
                Value::String(value) => value.as_bytes(),
                Value::Binary(value) => value.as_slice(),
                Value::Uuid(value) => value.as_bytes(),
                _ => return Err(error(py, &RetainedQueryError::UnsupportedType)),
            },
            _ => return Err(error(py, &RetainedQueryError::UnsupportedType)),
        };
        Ok(NodeBufferSpec {
            data: bytes.as_ptr().cast_mut().cast(),
            elements: bytes.len(),
            itemsize: 1,
            format: c"B",
        })
    }
}

struct NodeBufferSpec {
    data: *mut c_void,
    elements: usize,
    itemsize: usize,
    format: &'static CStr,
}
impl NodeBufferSpec {
    fn scalar<T>(value: &T, format: &'static CStr) -> Self {
        Self {
            data: std::ptr::from_ref(value).cast_mut().cast(),
            elements: 1,
            itemsize: std::mem::size_of::<T>(),
            format,
        }
    }
}

struct NodeBufferLease {
    _owner: NativeView,
    shape: isize,
    stride: isize,
}

#[pymethods]
impl RetainedNodeView {
    fn field(&self, py: Python<'_>, name: &str) -> PyResult<Option<Self>> {
        self.required(py)?;
        let child = match &self.location {
            Location::Node => match name {
                "_id" => Some(Child::Identity),
                "labels" => Some(Child::Labels),
                _ => self
                    .required(py)?
                    .properties()
                    .map_err(|source| error(py, &source))?
                    .position(|(key, _)| key == name)
                    .map(Child::Value),
            },
            Location::Value(_) => match self.value(py)? {
                Value::Map(values) => values.keys().position(|key| key == name).map(Child::Value),
                _ => return Err(error(py, &RetainedQueryError::UnsupportedType)),
            },
            _ => return Err(error(py, &RetainedQueryError::UnsupportedType)),
        };
        child.map(|child| self.child(py, child)).transpose()
    }
    fn item(&self, py: Python<'_>, index: usize) -> PyResult<Self> {
        self.required(py)?;
        let child = match self.location {
            Location::Labels => match self
                .required(py)?
                .field("labels")
                .map_err(|source| error(py, &source))?
            {
                Some(RetainedNodeField::Labels(labels)) if index < labels.iter().len() => {
                    Child::Label(index)
                }
                _ => return Err(PyIndexError::new_err("node label index out of bounds")),
            },
            Location::Value(_) => match self.value(py)? {
                Value::List(values) if index < values.len() => Child::Value(index),
                Value::List(_) => {
                    return Err(PyIndexError::new_err("node list index out of bounds"))
                }
                _ => return Err(error(py, &RetainedQueryError::UnsupportedType)),
            },
            _ => return Err(error(py, &RetainedQueryError::UnsupportedType)),
        };
        self.child(py, child)
    }
    fn scalar_copy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let owner = self.required(py)?;
        if matches!(self.location, Location::Identity) {
            let Some(RetainedNodeField::Identity(id)) =
                owner.field("_id").map_err(|source| error(py, &source))?
            else {
                return Err(error(py, &RetainedQueryError::InvalidColumn));
            };
            return crate::value::value_to_py(py, &Value::Int(id));
        }
        let value = self.value(py)?;
        if !matches!(
            value,
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_)
        ) {
            return Err(error(py, &RetainedQueryError::CopyRequired));
        }
        crate::value::value_to_py(py, value)
    }
    fn retain(&self, py: Python<'_>) -> PyResult<Self> {
        let path_bytes = match &self.location {
            Location::Value(path) => std::mem::size_of_val(path.as_ref()),
            _ => 0,
        };
        let bytes = path_bytes
            .checked_add(metadata::<Self>(py)?)
            .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))?;
        let inner = self
            .required(py)?
            .try_retain_with_metadata(bytes)
            .map_err(|source| error(py, &source))?;
        Ok(Self {
            location: self.location.clone(),
            inner: Some(inner),
            module: clone_module(py, &self.module),
        })
    }
    fn close(&mut self) {
        self.location = Location::Node;
        self.inner.take();
    }
    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<&'static str> {
        Ok(status(self.required(py)?.status()))
    }
    #[getter]
    fn kind(&self, py: Python<'_>) -> PyResult<&'static str> {
        self.required(py)?;
        Ok(match self.location {
            Location::Node => "node",
            Location::Identity => "int",
            Location::Labels => "list",
            Location::Label(_) => "utf8",
            Location::Value(_) => match self.value(py)? {
                Value::Null => "null",
                Value::Bool(_) => "bool",
                Value::Int(_) => "int",
                Value::Float(_) => "float",
                Value::String(_) => "utf8",
                Value::Binary(_) => "binary",
                Value::Uuid(_) => "uuid",
                Value::List(_) => "list",
                Value::Map(_) => "map",
            },
        })
    }
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit_module(&self.module, visit)
    }
    fn __clear__(&mut self) {
        self.close();
        self.module = None;
    }
    /// # Safety
    /// CPython supplies a writable Py_buffer. Its internal lease keeps the
    /// original immutable row and label allocations alive until release.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        if view.is_null() {
            return Err(PyBufferError::new_err("null buffer output"));
        }
        unsafe {
            view.write(ffi::Py_buffer::new());
        }
        check_buffer_flags(flags)?;
        let exporter = slf.try_borrow()?;
        let spec = exporter.buffer(py)?;
        let length = spec
            .elements
            .checked_mul(spec.itemsize)
            .and_then(|bytes| isize::try_from(bytes).ok())
            .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))?;
        let shape = isize::try_from(spec.elements)
            .map_err(|_| error(py, &RetainedQueryError::SizeOverflow))?;
        let memoryview_bytes = metadata::<PyMemoryView>(py)?;
        let bytes = std::mem::size_of::<NodeBufferLease>()
            .checked_add(std::mem::size_of::<ffi::Py_buffer>())
            .and_then(|bytes| bytes.checked_add(memoryview_bytes))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| error(py, &RetainedQueryError::SizeOverflow))?;
        let owner = exporter
            .required(py)?
            .try_retain_with_metadata(bytes)
            .map_err(|source| error(py, &source))?;
        let mut lease = Box::new(NodeBufferLease {
            _owner: owner,
            shape,
            stride: spec.itemsize as isize,
        });
        let mut descriptor = ffi::Py_buffer::new();
        descriptor.buf = spec.data;
        descriptor.len = length;
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
        unsafe {
            view.write(descriptor);
        }
        Ok(())
    }
    /// # Safety
    /// The descriptor comes from this exporter's successful getbuffer call.
    unsafe fn __releasebuffer__(_slf: Bound<'_, Self>, view: *mut ffi::Py_buffer) {
        if !view.is_null() {
            let internal = unsafe { (*view).internal };
            if !internal.is_null() {
                unsafe {
                    (*view).internal = ptr::null_mut();
                    drop(Box::from_raw(internal.cast::<NodeBufferLease>()));
                }
            }
        }
    }
}

pub(crate) fn node_options(
    py: Python<'_>,
    mut options: RetainedQueryOptions,
) -> PyResult<RetainedQueryOptions> {
    options.adapter_metadata_bytes =
        metadata::<RetainedNodeCursor>(py)?.max(metadata::<RetainedNodeView>(py)?);
    options.minimum_shared_handles = NonZeroUsize::new(4).expect("nonzero node protocol minimum");
    Ok(options)
}
