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

use std::alloc::{alloc, dealloc, Layout};
use std::ffi::c_void;
use std::io;
use std::ptr::{self, NonNull};
use zstd::stream::raw::{InBuffer, Operation, OutBuffer, WriteBuf};
use zstd::zstd_safe::zstd_sys as sys;

const QUALIFIED_VERSION: u32 = 10507;
const _: () = assert!(
    sys::ZSTD_VERSION_NUMBER == QUALIFIED_VERSION,
    "requalify the Zstd custom-memory ABI before updating its bindings"
);

// These two static-only declarations match zstd 1.5.7's zstd.h. Keep their
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

// Zstd needs malloc-compatible alignment, including its SIMD workspace.
// The prefix has the same alignment, so advancing past it preserves alignment.
#[repr(C, align(16))]
struct AllocationHeader {
    size: usize,
}

pub(super) unsafe extern "C" fn allocate(_opaque: *mut c_void, size: usize) -> *mut c_void {
    let Some(size) = size.checked_add(size_of::<AllocationHeader>()) else {
        return ptr::null_mut();
    };
    let Ok(layout) = Layout::from_size_align(size, align_of::<AllocationHeader>()) else {
        return ptr::null_mut();
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
    unsafe { header.write(AllocationHeader { size }) };
    #[cfg(test)]
    super::tests::record_allocation(size, true);
    // SAFETY: even a zero-byte request owns the nonzero header allocation.
    unsafe { header.add(1).cast() }
}

pub(super) unsafe extern "C" fn free(_opaque: *mut c_void, address: *mut c_void) {
    if address.is_null() {
        return;
    }
    // SAFETY: Zstd returns only live pointers from allocate, once each.
    let header = unsafe { address.cast::<AllocationHeader>().sub(1) };
    let size = unsafe { (*header).size };
    // SAFETY: allocate validated this exact size and alignment before storing it.
    let layout = unsafe { Layout::from_size_align_unchecked(size, align_of::<AllocationHeader>()) };
    unsafe { dealloc(header.cast(), layout) };
    #[cfg(test)]
    super::tests::record_allocation(size, false);
}

fn require_version() -> io::Result<()> {
    if zstd::zstd_safe::version_number() == QUALIFIED_VERSION {
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
    pointer: NonNull<sys::ZSTD_CCtx>,
    failed: bool,
}

impl CompressionContext {
    pub(super) fn new(level: i32) -> io::Result<Self> {
        require_version()?;
        // SAFETY: the statically linked, qualified ABI copies process-lifetime
        // callbacks. They have no borrowed opaque state or thread-local owner.
        let pointer = NonNull::new(unsafe { ZSTD_createCCtx_advanced(custom_memory()) })
            .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let context = Self {
            pointer,
            failed: false,
        };
        // SAFETY: the newly owned context is idle and has no dictionary.
        native_result(unsafe {
            sys::ZSTD_CCtx_setParameter(
                context.pointer.as_ptr(),
                sys::ZSTD_cParameter::ZSTD_c_compressionLevel,
                level,
            )
        })?;
        Ok(context)
    }

    fn step<C: WriteBuf + ?Sized>(
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
                        sys::ZSTD_compressStream(self.pointer.as_ptr(), output, input)
                    }
                    sys::ZSTD_EndDirective::ZSTD_e_flush => {
                        sys::ZSTD_flushStream(self.pointer.as_ptr(), output)
                    }
                    sys::ZSTD_EndDirective::ZSTD_e_end => {
                        sys::ZSTD_endStream(self.pointer.as_ptr(), output)
                    }
                }
            }
        });
        self.failed = result.is_err();
        result
    }
}

impl Operation for CompressionContext {
    fn run<C: WriteBuf + ?Sized>(
        &mut self,
        input: &mut InBuffer<'_>,
        output: &mut OutBuffer<'_, C>,
    ) -> io::Result<usize> {
        self.step(input, output, sys::ZSTD_EndDirective::ZSTD_e_continue)
    }

    fn flush<C: WriteBuf + ?Sized>(&mut self, output: &mut OutBuffer<'_, C>) -> io::Result<usize> {
        self.step(
            &mut InBuffer::around(&[]),
            output,
            sys::ZSTD_EndDirective::ZSTD_e_flush,
        )
    }

    fn finish<C: WriteBuf + ?Sized>(
        &mut self,
        output: &mut OutBuffer<'_, C>,
        _finished_frame: bool,
    ) -> io::Result<usize> {
        self.step(
            &mut InBuffer::around(&[]),
            output,
            sys::ZSTD_EndDirective::ZSTD_e_end,
        )
    }
}

impl Drop for CompressionContext {
    fn drop(&mut self) {
        // SAFETY: this uniquely owned context is freed through its stored callbacks.
        unsafe { sys::ZSTD_freeCCtx(self.pointer.as_ptr()) };
    }
}

pub struct DecompressionContext {
    pointer: NonNull<sys::ZSTD_DCtx>,
    failed: bool,
    prefix: [u8; 4],
    prefix_length: usize,
    prefix_position: usize,
}

impl DecompressionContext {
    pub fn new() -> io::Result<Self> {
        require_version()?;
        // SAFETY: same qualified ABI and callback lifetime as CompressionContext.
        let pointer = NonNull::new(unsafe { ZSTD_createDCtx_advanced(custom_memory()) })
            .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let context = Self {
            pointer,
            failed: false,
            prefix: [0; 4],
            prefix_length: 0,
            prefix_position: 0,
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
        self.prefix_length = 0;
        self.prefix_position = 0;
        Ok(())
    }

    pub fn sizeof(&self) -> usize {
        // SAFETY: the context is live and immutable during this read.
        unsafe { sys::ZSTD_sizeof_DCtx(self.pointer.as_ptr()) }
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
        // Zstd's pre-1.0 legacy decoders allocate through libc rather than
        // customMem. HawDB writes modern frames; reject legacy magic before
        // native decoding can enter that separate allocation path.
        if self.prefix_length < self.prefix.len() {
            let count = (self.prefix.len() - self.prefix_length).min(input.src.len() - input.pos());
            self.prefix[self.prefix_length..self.prefix_length + count]
                .copy_from_slice(&input.src[input.pos()..input.pos() + count]);
            self.prefix_length += count;
            input.set_pos(input.pos() + count);
            if self.prefix_length < self.prefix.len() {
                return Ok(self.prefix.len() - self.prefix_length);
            }
            let magic = u32::from_le_bytes(self.prefix);
            if magic != 0xfd2fb528 && magic & 0xfffffff0 != 0x184d2a50 {
                self.failed = true;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Zstd frame magic",
                ));
            }
        }
        if self.prefix_position < self.prefix.len() {
            let mut prefix = InBuffer::around(&self.prefix);
            prefix.set_pos(self.prefix_position);
            let result = stream(&mut prefix, output, |input, output| {
                // SAFETY: only validated modern/skippable magic is forwarded;
                // the context and both descriptors are exclusively borrowed.
                unsafe { sys::ZSTD_decompressStream(self.pointer.as_ptr(), output, input) }
            });
            self.prefix_position = prefix.pos();
            self.failed = result.is_err();
            return result;
        }
        let result = stream(input, output, |input, output| {
            // SAFETY: the context is exclusively borrowed and both buffer
            // descriptors are valid for this call.
            unsafe { sys::ZSTD_decompressStream(self.pointer.as_ptr(), output, input) }
        });
        self.failed = result.is_err();
        if matches!(result, Ok(0)) {
            self.prefix_length = 0;
            self.prefix_position = 0;
        }
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
// access only reads context size. Neither context borrows a dictionary or owner.
unsafe impl Send for CompressionContext {}
unsafe impl Sync for CompressionContext {}
unsafe impl Send for DecompressionContext {}
unsafe impl Sync for DecompressionContext {}
