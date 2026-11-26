// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::frame::FrameHeader;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use hawdb_core::RuntimeMemoryPermit;
use std::fmt;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};

/// Streaming encoder operation. Flush preserves the current frame; Finish ends it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeOperation {
    Continue,
    Flush,
    Finish,
}

/// Progress in the supplied slices. Zero `remaining` completes a decoder frame,
/// or the requested encoder Flush/Finish operation. A nonzero value is a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecProgress {
    pub consumed: usize,
    pub produced: usize,
    pub remaining: usize,
}

impl CodecProgress {
    pub(super) fn checked(self, input: usize, output: usize) -> io::Result<Self> {
        if self.consumed > input || self.produced > output {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compression backend reported an invalid buffer position",
            ));
        }
        Ok(self)
    }
}

/// An owned streaming Zstd encoder. A failed operation invalidates the context.
pub trait CompressionEncoder: Send + Sync {
    /// Set the exact length of the next frame while the context is idle.
    fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()>;
    fn step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        operation: EncodeOperation,
    ) -> io::Result<CodecProgress>;
    /// Release workspace before returning success, including callback failures.
    fn close(self: Box<Self>) -> io::Result<()>;
}

/// An owned streaming Zstd decoder. Reset must discard all failed/frame state.
pub trait CompressionDecoder: Send + Sync {
    fn reset(&mut self) -> io::Result<()>;
    /// Retained backend memory, including context, workspace and bookkeeping.
    fn retained_bytes(&self) -> usize;
    fn step(&mut self, input: &[u8], output: &mut [u8]) -> io::Result<CodecProgress>;
}

struct StateMemory {
    permit: Option<Box<dyn RuntimeMemoryPermit>>,
    work: CheckpointWorkContext,
    bytes: usize,
}

impl StateMemory {
    fn reserve(work: Option<&CheckpointWorkContext>, bytes: usize) -> io::Result<Option<Self>> {
        let Some(work) = work else {
            return Ok(None);
        };
        let permit = catch_unwind(AssertUnwindSafe(|| work.reserve_memory(bytes)))
            .map_err(|_| Self::panicked(work, bytes, "allocation"))?
            .map_err(io::Error::other)?;
        Ok(permit.map(|permit| Self {
            permit: Some(permit),
            work: work.clone(),
            bytes,
        }))
    }

    fn panicked(work: &CheckpointWorkContext, bytes: usize, phase: &str) -> io::Error {
        io::Error::other(work.record_failure(CheckpointWorkError::Allocation {
            bytes: bytes as u64,
            reason: format!("compression state memory callback panicked during {phase}"),
        }))
    }

    fn release(&mut self) -> io::Result<()> {
        let permit = self.permit.take();
        catch_unwind(AssertUnwindSafe(|| drop(permit)))
            .map_err(|_| Self::panicked(&self.work, self.bytes, "release"))
    }
}

impl Drop for StateMemory {
    fn drop(&mut self) {
        // Physical context drop precedes this field. Preserve failure evidence
        // without allowing a host permit panic to interrupt remaining cleanup.
        let _ = self.release();
    }
}

/// Owns an encoder and, for checkpoints, admission for its boxed Rust state.
/// The box is physically released before its memory permit is refunded.
pub struct EncoderState {
    pub(super) encoder: Box<dyn CompressionEncoder>,
    memory: Option<StateMemory>,
}

impl EncoderState {
    pub fn new<E: CompressionEncoder + 'static>(
        encoder: E,
        work: Option<&CheckpointWorkContext>,
    ) -> io::Result<Self> {
        let memory = StateMemory::reserve(work, size_of::<E>())?;
        Ok(Self {
            encoder: Box::new(encoder),
            memory,
        })
    }

    pub(super) fn close(self) -> io::Result<()> {
        let result = self.encoder.close();
        let released = self.memory.map(|mut memory| memory.release()).transpose();
        result.and(released.map(|_| ()))
    }
}

/// Owns a decoder and admission for its boxed Rust state, with physical release
/// preceding the permit refund. Backend bounds must include this boxed state.
pub struct DecoderState {
    pub(super) decoder: Box<dyn CompressionDecoder>,
    pub(super) state_bytes: usize,
    _memory: Option<StateMemory>,
}

impl DecoderState {
    pub fn new<D: CompressionDecoder + 'static>(
        decoder: D,
        work: Option<&CheckpointWorkContext>,
    ) -> io::Result<Self> {
        let memory = StateMemory::reserve(work, size_of::<D>())?;
        Ok(Self {
            decoder: Box::new(decoder),
            state_bytes: size_of::<D>(),
            _memory: memory,
        })
    }
}

/// Process-wide factory for compatible Zstd implementations.
///
/// All frames must interoperate with ordinary Zstd, including concatenated and
/// skippable frames. No native dictionaries or workers are requested. Bounds
/// must cover peak backend-owned allocations, including bookkeeping and buffer
/// replacement overlap. The caller reserves these bounds before construction
/// or growth. Rust I/O staging is reserved separately.
///
/// When `work` is present, acquire and retain its memory permits before physical
/// allocation and refund only after release. Preserve its typed errors in the
/// returned I/O error. Implementations own their allocator/bound correctness;
/// HawDB cannot intercept arbitrary foreign-library allocations.
pub trait CompressionBackend: fmt::Debug + Send + Sync + 'static {
    fn encoder(&self, level: i32, work: Option<&CheckpointWorkContext>)
        -> io::Result<EncoderState>;
    fn decoder(&self, work: Option<&CheckpointWorkContext>) -> io::Result<DecoderState>;
    fn encoder_workspace_bytes(&self, level: i32) -> io::Result<usize>;
    fn decoder_context_bytes(&self) -> io::Result<usize>;
    /// Additional peak workspace for this frame; previously retained buffers
    /// must fit the maximum of their previous bound and this bound.
    fn decoder_buffer_bytes(&self, header: &FrameHeader) -> io::Result<usize>;
    fn compress_bound(&self, input_len: usize) -> io::Result<usize>;
}

/// Another installation or the first use of the built-in backend already won.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionBackendAlreadyInitialized;

impl fmt::Display for CompressionBackendAlreadyInitialized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("compression backend is already initialized")
    }
}

impl std::error::Error for CompressionBackendAlreadyInitialized {}

enum BackendSelection {
    #[cfg(feature = "builtin-zstd")]
    Builtin,
    Host(Arc<dyn CompressionBackend>),
}

static BACKEND: OnceLock<BackendSelection> = OnceLock::new();

/// Install once, before the first compression use (including dictionary loading).
/// Concurrent installation/first use has exactly one winner. Replacement is
/// refused even after all databases close; existing contexts never switch.
pub fn install_compression_backend(
    backend: Arc<dyn CompressionBackend>,
) -> Result<(), CompressionBackendAlreadyInitialized> {
    BACKEND
        .set(BackendSelection::Host(backend))
        .map_err(|_| CompressionBackendAlreadyInitialized)
}

pub(super) fn selected() -> io::Result<&'static dyn CompressionBackend> {
    #[cfg(feature = "builtin-zstd")]
    let backend = BACKEND.get_or_init(|| BackendSelection::Builtin);
    #[cfg(not(feature = "builtin-zstd"))]
    let backend = BACKEND.get().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "install a compression backend or enable the builtin-zstd feature",
        )
    })?;
    Ok(match backend {
        #[cfg(feature = "builtin-zstd")]
        BackendSelection::Builtin => &super::native::BuiltinZstdBackend,
        BackendSelection::Host(backend) => backend.as_ref(),
    })
}

pub fn encoder_workspace_bytes(level: i32) -> io::Result<usize> {
    selected()?.encoder_workspace_bytes(level)
}

pub fn decoder_context_bytes() -> io::Result<usize> {
    selected()?.decoder_context_bytes()
}

pub fn decoder_buffer_bytes(header: &FrameHeader) -> io::Result<usize> {
    selected()?.decoder_buffer_bytes(header)
}

pub(super) fn compress_bound(input_len: usize) -> io::Result<usize> {
    selected()?.compress_bound(input_len)
}
