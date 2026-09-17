//! Admit pinned zstd buffers before the native decoder sees each frame header.

use crate::build_control::checkpoint;
use crate::build_memory::{checked_add as add, BuildMemory};
use crate::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::io::{self, BufRead, Read};
use zstd::zstd_safe::{DCtx, InBuffer, OutBuffer, ResetDirective};

// zstd 1.5.7 DCtx includes its entropy tables and fixed block scratch. Its
// streaming in/out allocation is admitted separately from each frame header.
const CONTEXT_BYTES: usize = 256 * 1024;
const BLOCK_BYTES: u64 = 128 * 1024;
const DEFAULT_MAX_WINDOW: u64 = (1 << 27) + 1;

pub(crate) struct Decoder<'a, R> {
    input: R,
    context: DCtx<'static>,
    header: [u8; 18],
    position: usize,
    length: usize,
    boundary: bool,
    task: &'a RuntimeTaskContext,
    // Native state and input drop before their capacity owners.
    _context_memory: QueryMemoryLease,
    buffer_memory: QueryMemoryLease,
}

impl<'a, R: BufRead> Decoder<'a, R> {
    pub(crate) fn new(
        input: R,
        memory: &BuildMemory,
        task: &'a RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        super::compression::require_qualified_zstd("decode admission")?;
        let context_memory = memory.spool.reserve(CONTEXT_BYTES)?;
        let buffer_memory = memory.spool.reserve(0)?;
        let mut context = DCtx::try_create().ok_or_else(|| {
            SkeinError::Execution("cannot allocate search hydration decoder".into())
        })?;
        context.init().map_err(native_error)?;
        if context.sizeof() > CONTEXT_BYTES {
            return Err(SkeinError::Execution(
                "search hydration decoder context exceeded admission".into(),
            ));
        }
        Ok(Self {
            input,
            context,
            header: [0; 18],
            position: 0,
            length: 0,
            boundary: true,
            task,
            _context_memory: context_memory,
            buffer_memory,
        })
    }

    fn begin_frame(&mut self) -> io::Result<bool> {
        checkpoint(self.task).map_err(io::Error::other)?;
        if self.input.fill_buf()?.is_empty() {
            return Ok(false);
        }
        self.input.read_exact(&mut self.header[..5])?;
        let magic = u32::from_le_bytes(self.header[..4].try_into().unwrap());
        self.length = if magic & 0xfffffff0 == 0x184d2a50 {
            8
        } else if magic == 0xfd2fb528 {
            let descriptor = self.header[4];
            let single = descriptor & 0x20 != 0;
            let dictionary = [0, 1, 2, 4][(descriptor & 3) as usize];
            let content = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
            5 + usize::from(!single) + dictionary + content
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid zstd frame magic",
            ));
        };
        self.input.read_exact(&mut self.header[5..self.length])?;
        let required = frame_buffer_bytes(&self.header[..self.length]).map_err(io::Error::other)?;
        // Pinned native resizing frees the previous in/out allocation before
        // replacing it; otherwise it retains that allocation across frames.
        self.buffer_memory
            .grow(required.saturating_sub(self.buffer_memory.bytes()))
            .map_err(io::Error::other)?;
        self.context
            .reset(ResetDirective::SessionOnly)
            .map_err(native_error)?;
        self.position = 0;
        self.boundary = false;
        Ok(true)
    }
}

impl<R: BufRead> Read for Decoder<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            checkpoint(self.task).map_err(io::Error::other)?;
            if self.boundary && !self.begin_frame()? {
                return Ok(0);
            }
            let header = self.position < self.length;
            let input = if header {
                &self.header[self.position..self.length]
            } else {
                self.input.fill_buf()?
            };
            let empty = input.is_empty();
            let mut input = InBuffer::around(input);
            let limit = output.len().min(8192);
            let mut output = OutBuffer::around(&mut output[..limit]);
            let remaining = self
                .context
                .decompress_stream(&mut output, &mut input)
                .map_err(native_error)?;
            let consumed = input.pos();
            let written = output.pos();
            if header {
                self.position += consumed;
            } else {
                self.input.consume(consumed);
            }
            if self.context.sizeof() > CONTEXT_BYTES + self.buffer_memory.bytes() {
                return Err(io::Error::other(
                    "search hydration native buffers exceeded admission",
                ));
            }
            self.boundary = remaining == 0;
            if written > 0 {
                return Ok(written);
            }
            if !self.boundary && consumed == 0 {
                return Err(io::Error::new(
                    if empty {
                        io::ErrorKind::UnexpectedEof
                    } else {
                        io::ErrorKind::InvalidData
                    },
                    "search hydration zstd stream made no progress",
                ));
            }
        }
    }
}

fn native_error(code: usize) -> io::Error {
    io::Error::other(zstd::zstd_safe::get_error_name(code))
}

fn frame_buffer_bytes(header: &[u8]) -> Result<usize> {
    let magic = u32::from_le_bytes(header[..4].try_into().unwrap());
    if magic & 0xfffffff0 == 0x184d2a50 {
        return Ok(0);
    }
    let content = zstd::zstd_safe::get_frame_content_size(header)
        .map_err(|_| SkeinError::Storage("invalid search hydration zstd frame header".into()))?;
    let window = if header[4] & 0x20 != 0 {
        content
            .ok_or_else(|| SkeinError::Storage("invalid single-segment zstd content size".into()))?
    } else {
        let descriptor = header[5];
        let base = 1u64 << (10 + (descriptor >> 3));
        base + (base >> 3) * u64::from(descriptor & 7)
    };
    // Preserve the existing decoder's default limit, including its +1 spelling.
    if window > DEFAULT_MAX_WINDOW {
        return Err(SkeinError::Storage(
            "search hydration zstd window exceeds the native limit".into(),
        ));
    }
    let block = window.min(BLOCK_BYTES);
    // ZSTD_decodingBufferSize_internal: min(content, window + 2*block +
    // 2*WILDCOPY_OVERLENGTH). The native decoder raises a tiny window to 1 KiB.
    let output = content
        .unwrap_or(u64::MAX)
        .min(window.max(1024) + 2 * block + 64);
    add(block.max(4) as usize, output as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn admitted_decoder_preserves_frames_and_releases_native_owners() {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut bytes = zstd::stream::encode_all(&b"first"[..], 3).unwrap();
        bytes.extend_from_slice(&0x184d2a5fu32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(b"abc");
        bytes.extend(zstd::stream::encode_all(&b"second"[..], 3).unwrap());
        let oracle = zstd::stream::decode_all(bytes.as_slice()).unwrap();
        for capacity in [1, 5, 8192] {
            let input = BufReader::with_capacity(capacity, Cursor::new(&bytes));
            let mut decoder = Decoder::new(input, &memory, &task).unwrap();
            let mut output = Vec::new();
            decoder.read_to_end(&mut output).unwrap();
            assert_eq!(output, oracle);
            assert!(decoder.context.sizeof() <= CONTEXT_BYTES + decoder.buffer_memory.bytes());
            drop(decoder);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }

    #[test]
    fn decoder_admits_actual_windows_and_rejects_one_short_before_native_growth() {
        use skein_core::RuntimeMemoryReservation;
        use std::io::Write;
        let original = vec![b'x'; 1023];
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.write_all(&original).unwrap();
        let compressed = encoder.finish().unwrap();
        assert_eq!(compressed[4] & 0x20, 0);
        for window in [0, 7, 56, 63, 88, 95] {
            let mut bytes = compressed.clone();
            bytes[5] = window;
            // A noncanonical four-byte zero dictionary ID remains valid.
            bytes[4] |= 3;
            bytes.splice(6..6, [0; 4]);
            assert_eq!(
                zstd::stream::decode_all(bytes.as_slice()).unwrap(),
                original
            );
            let required = frame_buffer_bytes(&bytes[..10]).unwrap();
            for short in [false, true] {
                let budget = CONTEXT_BYTES + required - usize::from(short);
                let task = RuntimeTaskContext::default()
                    .with_memory_reservation(RuntimeMemoryReservation::new(budget as u64, 0));
                let memory = BuildMemory::new(&task).unwrap();
                let mut decoder = Decoder::new(Cursor::new(&bytes), &memory, &task).unwrap();
                let mut output = Vec::new();
                let result = decoder.read_to_end(&mut output);
                if short {
                    assert!(result.unwrap_err().to_string().contains("memory"));
                    assert!(output.is_empty());
                    assert!(decoder.context.sizeof() <= CONTEXT_BYTES);
                } else {
                    result.unwrap();
                    assert_eq!(output, original);
                }
                drop(decoder);
                assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            }
        }
    }

    #[test]
    fn decoder_preserves_content_size_frames_and_rejects_reserved_lengths() {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        for size in [0, 1, 255, 256, 65535, 65536] {
            let input = vec![b'x'; size];
            let bytes = zstd::bulk::compress(&input, 3).unwrap();
            let mut decoder = Decoder::new(Cursor::new(bytes), &memory, &task).unwrap();
            let mut output = Vec::new();
            decoder.read_to_end(&mut output).unwrap();
            assert_eq!(output, input);
            drop(decoder);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
        for size in [u64::MAX, u64::MAX - 1, 1 << 32] {
            let mut header = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
            header.extend_from_slice(&size.to_le_bytes());
            assert!(frame_buffer_bytes(&header).is_err());
            let mut decoder = Decoder::new(Cursor::new(header), &memory, &task).unwrap();
            assert!(decoder.read(&mut [0; 1]).is_err());
            drop(decoder);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}
