// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ffi::c_int;
use std::fmt::Write;

/// Standard Arrow C Stream layout. The raw consumer owns one active descriptor
/// and must release it once; arrays/schemas outlive the stream independently.
#[repr(C)]
#[derive(Debug, Default)]
pub struct ArrowArrayStream {
    pub get_schema: Option<unsafe extern "C" fn(*mut Self, *mut ArrowSchema) -> c_int>,
    pub get_next: Option<unsafe extern "C" fn(*mut Self, *mut ArrowArray) -> c_int>,
    pub get_last_error: Option<unsafe extern "C" fn(*mut Self) -> *const c_char>,
    pub release: Option<unsafe extern "C" fn(*mut Self)>,
    pub private_data: *mut c_void,
}

/// Owned consumer-driven stream over the root numeric cursor. No pull occurs
/// during construction/schema export. Native pressure is a nonzero Arrow error.
#[derive(Debug)]
#[repr(transparent)]
pub struct RetainedArrowStream(ArrowArrayStream);
impl RetainedArrowStream {
    /// Pre-admits/allocates the stream before taking the original cursor. Any
    /// failure leaves the option and its source position unchanged. Each child
    /// descriptor receives the same independently retained code/module owner.
    pub fn try_take_cursor(
        cursor: &mut Option<RetainedQueryCursor>,
        wrapper_bytes: usize,
        code: Option<Arc<dyn RetainedArrowCodeOwner>>,
    ) -> Result<Self> {
        let native = cursor.as_ref().ok_or(RetainedQueryError::Closed)?;
        let count = native.schema().len();
        handles(
            &native.shared,
            count
                .checked_add(4)
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        if native
            .schema()
            .iter()
            .any(|field| field.name.as_bytes().contains(&0))
        {
            return Err(RetainedQueryError::UnsupportedLayout);
        }
        let extra = wrapper_bytes
            .checked_add(std::mem::size_of::<ArrowArrayStream>())
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let lease = admit(&native.shared, capacity::<StreamOwner>(extra)?, &code)?;
        let mut owner = Box::new(StreamOwner {
            state: Mutex::new(StreamState {
                cursor: None,
                message: ErrorText::default(),
                terminal: None,
            }),
            _lease: lease,
        });
        owner
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cursor = cursor.take();
        Ok(Self(ArrowArrayStream {
            get_schema: Some(get_schema),
            get_next: Some(get_next),
            get_last_error: Some(get_last_error),
            release: Some(release_stream),
            private_data: Box::into_raw(owner).cast(),
        }))
    }
    pub fn descriptor(&self) -> &ArrowArrayStream {
        &self.0
    }
    /// Metadata observation does not request/prefetch another batch.
    pub fn profile(&self) -> RetainedQueryProfile {
        let owner = unsafe { &*self.0.private_data.cast::<StreamOwner>() };
        let state = owner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cursor.as_ref().expect("active stream").profile()
    }
    pub fn status(&self) -> RetainedQueryStatus {
        let owner = unsafe { &*self.0.private_data.cast::<StreamOwner>() };
        let state = owner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cursor.as_ref().expect("active stream").status()
    }
    /// Transfer the standard move/release obligation to a raw consumer.
    pub fn into_raw(mut self) -> ArrowArrayStream {
        std::mem::take(&mut self.0)
    }
    /// Return the adopted cursor at its current position. This also lets a
    /// foreign adapter undo an export when its final wrapper allocation fails.
    pub fn into_cursor(mut self) -> RetainedQueryCursor {
        self.0.release = None;
        let pointer = std::mem::replace(&mut self.0.private_data, ptr::null_mut());
        let owner = unsafe { Box::from_raw(pointer.cast::<StreamOwner>()) };
        let StreamOwner { state, _lease } = *owner;
        let cursor = state
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cursor
            .expect("active stream");
        drop(_lease);
        cursor
    }
}
impl Drop for RetainedArrowStream {
    fn drop(&mut self) {
        if let Some(release) = self.0.release {
            unsafe { release(&mut self.0) };
        }
    }
}

impl RetainedQueryCursor {
    /// Consuming convenience wrapper; use try_take_cursor when a failed export
    /// must preserve ownership of this cursor for retry.
    pub fn into_arrow_stream(self, wrapper_bytes: usize) -> Result<RetainedArrowStream> {
        RetainedArrowStream::try_take_cursor(&mut Some(self), wrapper_bytes, None)
    }
}

struct StreamOwner {
    state: Mutex<StreamState>,
    _lease: MetadataLease,
}
struct StreamState {
    cursor: Option<RetainedQueryCursor>,
    // All errors produced by the supported heap numeric source/governor have
    // bounded diagnostic formats. This prepaid workspace prevents allocation
    // at a full retained allowance; overflow is explicit, never truncated.
    message: ErrorText,
    terminal: Option<ErrorText>,
}
#[derive(Clone)]
struct ErrorText {
    bytes: [u8; 4096],
    length: usize,
    code: c_int,
}
impl Default for ErrorText {
    fn default() -> Self {
        Self {
            bytes: [0; 4096],
            length: 0,
            code: 0,
        }
    }
}
impl Write for ErrorText {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        for part in value.split_inclusive('\0') {
            let text = part.strip_suffix('\0').unwrap_or(part);
            let escape = if part.ends_with('\0') {
                b"\\0".as_slice()
            } else {
                &[]
            };
            let end = self
                .length
                .checked_add(text.len())
                .and_then(|n| n.checked_add(escape.len()))
                .filter(|n| *n < self.bytes.len())
                .ok_or(std::fmt::Error)?;
            let split = self.length + text.len();
            self.bytes[self.length..split].copy_from_slice(text.as_bytes());
            self.bytes[split..end].copy_from_slice(escape);
            self.length = end;
            self.bytes[end] = 0;
        }
        Ok(())
    }
}
impl ErrorText {
    fn clear(&mut self) {
        self.length = 0;
        self.bytes[0] = 0;
        self.code = 0;
    }
    fn set(&mut self, error: &RetainedQueryError) -> c_int {
        self.clear();
        let code = if error.is_retryable() {
            12
        } else {
            match error {
                RetainedQueryError::UnsupportedPlan
                | RetainedQueryError::UnsupportedLayout
                | RetainedQueryError::UnsupportedType
                | RetainedQueryError::CopyRequired
                | RetainedQueryError::SelectionRequiresMaterialization
                | RetainedQueryError::InvalidColumn
                | RetainedQueryError::Closed
                | RetainedQueryError::SizeOverflow => 22,
                RetainedQueryError::WorkingUnitTooLarge
                | RetainedQueryError::RetainedAdmission(_)
                | RetainedQueryError::ResultBudget { .. }
                | RetainedQueryError::Admission(_) => 12,
                _ => 5,
            }
        };
        if write!(self, "{error}").is_err() {
            self.clear();
            self.write_str("Arrow stream error diagnostic exceeds prepaid capacity")
                .expect("bounded literal");
            self.code = 5;
        } else {
            self.code = code;
        }
        self.code
    }
}

// Safety: Arrow consumers supply aligned live descriptors and serialize release
// against callbacks. The mutex additionally serializes demanded source pulls.
unsafe fn owner<'a>(stream: *mut ArrowArrayStream) -> Option<&'a StreamOwner> {
    let stream = unsafe { stream.as_ref()? };
    if stream.release.is_none() || stream.private_data.is_null() {
        return None;
    }
    unsafe { stream.private_data.cast::<StreamOwner>().as_ref() }
}
unsafe extern "C" fn get_schema(stream: *mut ArrowArrayStream, output: *mut ArrowSchema) -> c_int {
    if output.is_null() {
        return 22;
    }
    unsafe { output.write(ArrowSchema::default()) };
    let Some(owner) = (unsafe { owner(stream) }) else {
        return 22;
    };
    let mut state = owner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let result = catch_unwind(AssertUnwindSafe(|| {
        let cursor = state.cursor.as_ref().expect("active stream");
        schema(&cursor.shared, 0, &owner._lease._code)
    }));
    match result {
        Ok(Ok(schema)) => {
            state.message.clear();
            unsafe { output.write(schema.into_raw()) };
            0
        }
        Ok(Err(error)) => state.message.set(&error),
        Err(_) => {
            state
                .cursor
                .as_mut()
                .expect("active stream")
                .abort_delivery();
            let code = state.message.set(&RetainedQueryError::AdapterDelivery);
            state.terminal = Some(state.message.clone());
            code
        }
    }
}
unsafe extern "C" fn get_next(stream: *mut ArrowArrayStream, output: *mut ArrowArray) -> c_int {
    if output.is_null() {
        return 22;
    }
    unsafe { output.write(ArrowArray::default()) };
    let Some(owner) = (unsafe { owner(stream) }) else {
        return 22;
    };
    let mut state = owner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(terminal) = &state.terminal {
        state.message = terminal.clone();
        return state.message.code;
    }
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<Option<ArrowArray>> {
        let cursor = state.cursor.as_mut().expect("active stream");
        match cursor.status() {
            RetainedQueryStatus::Completed => return Ok(None),
            RetainedQueryStatus::Failed | RetainedQueryStatus::Closed => {
                return cursor.next_batch().map(|_| None);
            }
            RetainedQueryStatus::Open => {}
        }
        if cursor.can_complete_without_output() {
            return cursor.next_batch().map(|_| None);
        }
        // Every array/child descriptor handle is admitted before native pull.
        // The native batch admits its own Arc/header before advancing source.
        let reservation = ArrayReservation::new(&cursor.shared, 0, &owner._lease._code)?;
        let Some(batch) = cursor.next_batch_with_metadata(2 * std::mem::size_of::<usize>() + 64)?
        else {
            return Ok(None);
        };
        let batch = Arc::new(batch);
        reservation.build(batch).map(Some)
    }));
    match result {
        Ok(Ok(Some(array))) => {
            state.message.clear();
            unsafe { output.write(array) };
            0
        }
        Ok(Ok(None)) => {
            state.message.clear();
            0
        }
        Ok(Err(error)) => {
            let code = state.message.set(&error);
            if !error.is_retryable() {
                state
                    .cursor
                    .as_mut()
                    .expect("active stream")
                    .abort_delivery();
                state.terminal = Some(state.message.clone());
            }
            code
        }
        Err(_) => {
            state
                .cursor
                .as_mut()
                .expect("active stream")
                .abort_delivery();
            let code = state.message.set(&RetainedQueryError::AdapterDelivery);
            state.terminal = Some(state.message.clone());
            code
        }
    }
}
unsafe extern "C" fn get_last_error(stream: *mut ArrowArrayStream) -> *const c_char {
    let Some(owner) = (unsafe { owner(stream) }) else {
        return ptr::null();
    };
    let state = owner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if state.message.code == 0 {
        ptr::null()
    } else {
        state.message.bytes.as_ptr().cast()
    }
}
unsafe extern "C" fn release_stream(stream: *mut ArrowArrayStream) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(stream) = unsafe { stream.as_mut() }
            && stream.release.take().is_some()
        {
            let data = std::mem::replace(&mut stream.private_data, ptr::null_mut());
            unsafe {
                drop(Box::from_raw(data.cast::<StreamOwner>()));
            }
        }
    }));
}
