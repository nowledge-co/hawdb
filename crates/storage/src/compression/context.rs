// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use hawdb_core::RuntimeMemoryPermit;
use std::alloc::{alloc, dealloc, Layout};
use std::ffi::c_void;
use std::io;
use std::ptr::{self, NonNull};
use zstd::stream::raw::{InBuffer, OutBuffer, WriteBuf};
use zstd::zstd_safe::zstd_sys as sys;

const fn qualified_version(version: u32) -> bool {
    matches!(version, 10507 | 10600)
}

const _: () = assert!(
    qualified_version(sys::ZSTD_VERSION_NUMBER),
    "requalify the Zstd custom-memory ABI before updating its bindings"
);

// These two static-only declarations match zstd 1.5.7 and 1.6.0's zstd.h. Keep their
// ABI private instead of enabling every experimental zstd-rs interface.
#[repr(C)]
struct CustomMemory {
    allocate: unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void,
    free: unsafe extern "C" fn(*mut c_void, *mut c_void),
    opaque: *mut c_void,
}

unsafe extern "C" {
    fn ZSTD_createCCtx_advanced(memory: CustomMemory) -> *mut sys::ZSTD_CCtx;
    fn ZSTD_createDCtx_advanced(memory: CustomMemory) -> *mut sys::ZSTD_DCtx;
}

fn custom_memory() -> CustomMemory {
    CustomMemory {
        allocate,
        free,
        opaque: ptr::null_mut(),
    }
}

struct MemoryContext {
    work: CheckpointWorkContext,
    failed: std::sync::atomic::AtomicBool,
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

// Zstd needs malloc-compatible alignment, including its SIMD workspace.
// The prefix has the same alignment, so advancing past it preserves alignment.
#[repr(C, align(16))]
struct AllocationHeader {
    size: usize,
    memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

pub(super) unsafe extern "C" fn allocate(opaque: *mut c_void, size: usize) -> *mut c_void {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: the native caller obeys the same qualified opaque lifetime
        // as allocate_inner. A host controller panic must not cross the C ABI.
        unsafe { allocate_inner(opaque, size) }
    })) {
        Ok(pointer) => pointer,
        Err(_) => {
            // SAFETY: opaque remains owned throughout the native operation.
            unsafe { record_callback_panic(opaque, size, "allocation") };
            ptr::null_mut()
        }
    }
}

unsafe fn record_callback_panic(opaque: *mut c_void, size: usize, phase: &str) {
    if !opaque.is_null() {
        // SAFETY: all callback entry points retain their stable MemoryContext.
        let context = unsafe { &*opaque.cast::<MemoryContext>() };
        context
            .failed
            .store(true, std::sync::atomic::Ordering::Release);
        let work = &context.work;
        work.record_failure(CheckpointWorkError::Allocation {
            bytes: size as u64,
            reason: format!("compression memory callback panicked during {phase}"),
        });
    }
}

unsafe fn allocate_inner(opaque: *mut c_void, size: usize) -> *mut c_void {
    let Some(size) = size.checked_add(size_of::<AllocationHeader>()) else {
        return ptr::null_mut();
    };
    let Ok(layout) = Layout::from_size_align(size, align_of::<AllocationHeader>()) else {
        return ptr::null_mut();
    };
    let memory = if opaque.is_null() {
        None
    } else {
        // SAFETY: the encoder owns this stable Box until after freeCCtx has
        // returned; Zstd invokes callbacks synchronously within its operations.
        let work = unsafe { &(*opaque.cast::<MemoryContext>()).work };
        match work.reserve_memory(size) {
            Ok(memory) => memory,
            Err(_) => return ptr::null_mut(),
        }
    };
    #[cfg(test)]
    if super::tests::reject_allocation() {
        return ptr::null_mut();
    }
    // SAFETY: the nonzero layout is checked, and the host's GlobalAlloc must
    // not unwind. Returning NULL lets Zstd report its own allocation failure.
    let allocation = unsafe { alloc(layout) };
    if allocation.is_null() {
        return ptr::null_mut();
    }
    let header = allocation.cast::<AllocationHeader>();
    // SAFETY: the prefix is aligned and fits in the newly allocated block.
    unsafe { header.write(AllocationHeader { size, memory }) };
    #[cfg(test)]
    super::tests::record_allocation(size, true);
    // SAFETY: even a zero-byte request owns the nonzero header allocation.
    unsafe { header.add(1).cast() }
}

pub(super) unsafe extern "C" fn free(opaque: *mut c_void, address: *mut c_void) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: native free supplies either NULL or one live prefix allocation.
        unsafe { free_inner(address) }
    }))
    .is_err()
    {
        // SAFETY: native free has not returned, so its opaque owner is still live.
        unsafe { record_callback_panic(opaque, 0, "release") };
    }
}

unsafe fn free_inner(address: *mut c_void) {
    if address.is_null() {
        return;
    }
    // SAFETY: Zstd returns only live pointers from allocate, once each.
    let header = unsafe { address.cast::<AllocationHeader>().sub(1) };
    let ownership = unsafe { header.read() };
    let size = ownership.size;
    // SAFETY: allocate validated this exact size and alignment before storing it.
    let layout = unsafe { Layout::from_size_align_unchecked(size, align_of::<AllocationHeader>()) };
    unsafe { dealloc(header.cast(), layout) };
    #[cfg(test)]
    super::tests::record_allocation(size, false);
    // Physical allocation dies before its working-memory permit is refunded.
    drop(ownership.memory);
}

pub(super) fn require_version() -> io::Result<()> {
    validate_version(zstd::zstd_safe::version_number())
}

fn validate_version(linked: u32) -> io::Result<()> {
    // A qualified binding does not qualify a substituted native library.
    if qualified_version(linked) && linked == sys::ZSTD_VERSION_NUMBER {
        Ok(())
    } else {
        Err(io::Error::other("unqualified Zstd custom-memory ABI"))
    }
}

fn native_result(code: usize) -> io::Result<usize> {
    // SAFETY: this predicate accepts every size_t returned by Zstd.
    if unsafe { sys::ZSTD_isError(code) } == 0 {
        Ok(code)
    } else {
        Err(io::Error::other(zstd::zstd_safe::get_error_name(code)))
    }
}

fn stream<C: WriteBuf + ?Sized>(
    input: &mut InBuffer<'_>,
    output: &mut OutBuffer<'_, C>,
    run: impl FnOnce(*mut sys::ZSTD_inBuffer, *mut sys::ZSTD_outBuffer) -> usize,
) -> io::Result<usize> {
    if input.pos() > input.src.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Zstd input position",
        ));
    }
    let mut native_input = sys::ZSTD_inBuffer {
        src: input.src.as_ptr().cast(),
        size: input.src.len(),
        pos: input.pos(),
    };
    let mut native_output = sys::ZSTD_outBuffer {
        dst: output.as_mut_ptr().cast(),
        size: output.capacity(),
        pos: output.pos(),
    };
    let code = run(&mut native_input, &mut native_output);
    input.set_pos(native_input.pos);
    // SAFETY: Zstd initialized all bytes up to pos, within the supplied buffer.
    unsafe { output.set_pos(native_output.pos) };
    native_result(code)
}

pub(super) struct CompressionContext {
    pointer: Option<NonNull<sys::ZSTD_CCtx>>,
    failed: bool,
    _memory_context: Option<Box<MemoryContext>>,
}

impl CompressionContext {
    pub(super) fn new(level: i32) -> io::Result<Self> {
        Self::with_memory(level, None)
    }

    pub(super) fn new_with_work_context(
        level: i32,
        work: &CheckpointWorkContext,
    ) -> io::Result<Self> {
        let memory = work
            .reserve_memory(size_of::<MemoryContext>())
            .map_err(io::Error::other)?;
        let context = Box::new(MemoryContext {
            work: work.clone(),
            failed: std::sync::atomic::AtomicBool::new(false),
            _memory: memory,
        });
        Self::with_memory(level, Some(context))
    }

    fn with_memory(level: i32, mut memory: Option<Box<MemoryContext>>) -> io::Result<Self> {
        require_version()?;
        let mut callbacks = custom_memory();
        if let Some(context) = memory.as_mut() {
            callbacks.opaque = (&mut **context as *mut MemoryContext).cast();
        }
        // SAFETY: the statically linked, qualified ABI copies process-lifetime
        // callbacks. The optional opaque Box remains stable and owned until
        // after the native context is freed, including initialization failures.
        let pointer = NonNull::new(unsafe { ZSTD_createCCtx_advanced(callbacks) })
            .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let context = Self {
            pointer: Some(pointer),
            failed: false,
            _memory_context: memory,
        };
        // SAFETY: the newly owned context is idle and has no dictionary.
        native_result(unsafe {
            sys::ZSTD_CCtx_setParameter(
                context.pointer(),
                sys::ZSTD_cParameter::ZSTD_c_compressionLevel,
                level,
            )
        })?;
        Ok(context)
    }

    pub(super) fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        // SAFETY: the idle context is exclusively owned; Zstd checks the
        // pledged byte count when finishing the next frame.
        native_result(unsafe { sys::ZSTD_CCtx_setPledgedSrcSize(self.pointer(), bytes) })?;
        Ok(())
    }

    fn pointer(&self) -> *mut sys::ZSTD_CCtx {
        self.pointer
            .expect("owned compression context is live")
            .as_ptr()
    }

    fn release_native(&mut self) {
        if let Some(pointer) = self.pointer.take() {
            // SAFETY: the context and optional opaque owner remain live;
            // taking the pointer fences every path against a second free.
            unsafe { sys::ZSTD_freeCCtx(pointer.as_ptr()) };
        }
    }

    pub(super) fn close(mut self) -> io::Result<()> {
        self.release_native();
        if self
            ._memory_context
            .as_ref()
            .is_some_and(|context| context.failed.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(io::Error::other("compression memory callback failed"));
        }
        Ok(())
    }

    pub(super) fn step<C: WriteBuf + ?Sized>(
        &mut self,
        input: &mut InBuffer<'_>,
        output: &mut OutBuffer<'_, C>,
        directive: sys::ZSTD_EndDirective,
    ) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other(
                "Zstd encoder requires reset after failure",
            ));
        }
        let result = stream(input, output, |input, output| {
            // SAFETY: the context is exclusively borrowed, has not failed,
            // and both buffer descriptors are valid for this call.
            unsafe {
                match directive {
                    sys::ZSTD_EndDirective::ZSTD_e_continue => {
                        sys::ZSTD_compressStream(self.pointer(), output, input)
                    }
                    sys::ZSTD_EndDirective::ZSTD_e_flush => {
                        sys::ZSTD_flushStream(self.pointer(), output)
                    }
                    sys::ZSTD_EndDirective::ZSTD_e_end => {
                        sys::ZSTD_endStream(self.pointer(), output)
                    }
                }
            }
        });
        self.failed = result.is_err();
        result
    }
}

impl Drop for CompressionContext {
    fn drop(&mut self) {
        self.release_native();
    }
}

pub struct DecompressionContext {
    pointer: NonNull<sys::ZSTD_DCtx>,
    initial_bytes: usize,
    failed: bool,
    _memory_context: Option<Box<MemoryContext>>,
}

impl DecompressionContext {
    pub fn new() -> io::Result<Self> {
        Self::with_memory(None)
    }

    pub(crate) fn new_with_work_context(work: &CheckpointWorkContext) -> io::Result<Self> {
        let memory = work
            .reserve_memory(size_of::<MemoryContext>())
            .map_err(io::Error::other)?;
        let context = Box::new(MemoryContext {
            work: work.clone(),
            failed: std::sync::atomic::AtomicBool::new(false),
            _memory: memory,
        });
        Self::with_memory(Some(context))
    }

    fn with_memory(mut memory: Option<Box<MemoryContext>>) -> io::Result<Self> {
        require_version()?;
        let mut callbacks = custom_memory();
        if let Some(context) = memory.as_mut() {
            callbacks.opaque = (&mut **context as *mut MemoryContext).cast();
        }
        // SAFETY: callbacks and their stable opaque owner remain live through
        // every native operation and until after freeDCtx has returned.
        let pointer = NonNull::new(unsafe { ZSTD_createDCtx_advanced(callbacks) })
            .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let context = Self {
            pointer,
            // SAFETY: the newly allocated native context is live and idle.
            initial_bytes: unsafe { sys::ZSTD_sizeof_DCtx(pointer.as_ptr()) },
            failed: false,
            _memory_context: memory,
        };
        // SAFETY: the newly owned context is idle.
        native_result(unsafe { sys::ZSTD_initDStream(context.pointer.as_ptr()) })?;
        Ok(context)
    }

    pub fn reset(&mut self) -> io::Result<()> {
        // SAFETY: resetting an exclusively owned context also recovers errors.
        native_result(unsafe {
            sys::ZSTD_DCtx_reset(
                self.pointer.as_ptr(),
                sys::ZSTD_ResetDirective::ZSTD_reset_session_only,
            )
        })?;
        self.failed = false;
        Ok(())
    }

    pub fn sizeof(&self) -> usize {
        // SAFETY: the context is live and immutable during this read.
        unsafe { sys::ZSTD_sizeof_DCtx(self.pointer.as_ptr()) }
    }

    pub(super) fn accounted_bytes(&self) -> usize {
        let native = self.sizeof();
        // No dictionaries/workers: one DCtx and, once needed, one combined
        // input/output allocation. Include our callback prefixes and owner box.
        native
            + (1 + usize::from(native > self.initial_bytes)) * size_of::<AllocationHeader>()
            + usize::from(self._memory_context.is_some()) * size_of::<MemoryContext>()
    }

    pub fn decompress_stream<C: WriteBuf + ?Sized>(
        &mut self,
        output: &mut OutBuffer<'_, C>,
        input: &mut InBuffer<'_>,
    ) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other(
                "Zstd decoder requires reset after failure",
            ));
        }
        if input.pos() > input.src.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid Zstd input position",
            ));
        }
        // The backend-neutral session validates modern/skippable magic before
        // forwarding bytes. This keeps libc-owned pre-1.0 decoders unreachable.
        let result = stream(input, output, |input, output| {
            // SAFETY: the context is exclusively borrowed and both buffer
            // descriptors are valid for this call.
            unsafe { sys::ZSTD_decompressStream(self.pointer.as_ptr(), output, input) }
        });
        self.failed = result.is_err();
        result
    }
}

impl Drop for DecompressionContext {
    fn drop(&mut self) {
        // SAFETY: this uniquely owned context is freed through its stored callbacks.
        unsafe { sys::ZSTD_freeDCtx(self.pointer.as_ptr()) };
    }
}

// SAFETY: callbacks use the immutable host global allocator, which supports all
// host threads. Mutating context operations require exclusive access; shared
// access only reads context size. The optional owned memory controller/permits
// are Send + Sync. Neither context borrows a dictionary or host owner.
unsafe impl Send for CompressionContext {}
unsafe impl Sync for CompressionContext {}
unsafe impl Send for DecompressionContext {}
unsafe impl Sync for DecompressionContext {}

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn only_qualified_bindings_with_the_matching_library_are_accepted() {
        for version in [10507, 10600] {
            assert!(qualified_version(version));
            assert_eq!(
                validate_version(version).is_ok(),
                version == sys::ZSTD_VERSION_NUMBER
            );
        }
        for version in [0, 10506, 10508, 10599, 10601, u32::MAX] {
            assert!(!qualified_version(version));
            assert!(validate_version(version).is_err());
        }
        require_version().unwrap();
    }
}
