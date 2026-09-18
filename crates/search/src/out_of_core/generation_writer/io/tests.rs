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

use super::*;
use hawdb_core::RuntimeMemoryReservation;
use std::cell::Cell;
use std::io::{self, Cursor};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-publication-io-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn context(bytes: usize) -> (BuildMemory, RuntimeTaskContext) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0));
    (BuildMemory::new(&task).unwrap(), task)
}

#[test]
fn file_read_admits_exact_output_and_scratch_before_buffer_allocation() {
    let root = Directory::new();
    let source = root.0.join("source");
    let bytes = vec![42; 3 * SPOOL_BUFFER_BYTES + 17];
    fs::write(&source, &bytes).unwrap();
    let held = 4096;
    let exact = held + bytes.len() + SPOOL_BUFFER_BYTES;
    for short in [0, 1] {
        let (memory, task) = context(exact - short);
        let other = memory.input.reserve(held).unwrap();
        let io = GenerationIo::new(&memory, &task);
        let result = io.read(&source, bytes.len() as u64);
        if short == 0 {
            let output = result.unwrap();
            assert_eq!(output.bytes, bytes);
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                held + output.bytes.capacity()
            );
            drop(output);
        } else {
            assert!(result.err().unwrap().to_string().contains("query memory"));
        }
        assert!(io.read(&source, bytes.len() as u64 - 1).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, held);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn copy_buffer_denial_precedes_output_creation_and_accepts_exact_root() {
    let root = Directory::new();
    let source = root.0.join("source");
    let target = root.0.join("target");
    let bytes = vec![37; SPOOL_BUFFER_BYTES + 11];
    fs::write(&source, &bytes).unwrap();
    let native = native_path::bytes(&source)
        .unwrap()
        .max(native_path::bytes(&target).unwrap());
    let held = 1024;
    for short in [1, 0] {
        let (memory, task) = context(held + SPOOL_BUFFER_BYTES + native - short);
        let other = memory.input.reserve(held).unwrap();
        let result = GenerationIo::new(&memory, &task).copy(&source, &target);
        if short == 1 {
            assert!(result.unwrap_err().to_string().contains("query memory"));
            assert!(!target.exists());
        } else {
            result.unwrap();
            assert_eq!(fs::read(&target).unwrap(), bytes);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, held);
        drop(other);
    }
}

#[test]
fn forced_copy_preserves_bytes_and_cancelled_fallback_keeps_old_target() {
    let root = Directory::new();
    let source = root.0.join("source");
    let target = root.0.join("target");
    let bytes = vec![91; 3 * SPOOL_BUFFER_BYTES + 3];
    fs::write(&source, &bytes).unwrap();
    for cancel in [true, false] {
        fs::write(&target, b"old generation").unwrap();
        let (memory, task) = context(256 * 1024);
        let result = GenerationIo::new(&memory, &task).link_with(&source, &target, |_, _| {
            if cancel {
                task.cancellation().cancel();
            }
            Err(io::Error::other("injected hard-link failure"))
        });
        if cancel {
            assert!(result.unwrap_err().to_string().contains("cancel"));
            assert_eq!(fs::read(&target).unwrap(), b"old generation");
        } else {
            result.unwrap();
            assert_eq!(fs::read(&target).unwrap(), bytes);
        }
        assert_eq!(fs::read_dir(&root.0).unwrap().count(), 2);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn temporary_cleanup_survives_a_full_root_and_unwind() {
    let root = Directory::new();
    let source = root.0.join("source");
    let target = root.0.join("target");
    fs::write(&source, b"new").unwrap();
    fs::write(&target, b"old").unwrap();
    let (memory, task) = context(256 * 1024);
    let full = std::cell::RefCell::new(None);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        GenerationIo::new(&memory, &task).link_with(&source, &target, |_, temporary| {
            fs::write(temporary, b"partial").unwrap();
            *full.borrow_mut() = Some(
                memory
                    .input
                    .reserve(256 * 1024 - memory.ledger.snapshot().used_bytes)
                    .unwrap(),
            );
            panic!("injected publication unwind");
        })
    }));
    assert!(result.is_err());
    assert_eq!(fs::read(&target).unwrap(), b"old");
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 2);
    drop(full.into_inner());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn cancellation_at_the_replace_fence_distinguishes_unpublished_and_committed_data() {
    let root = Directory::new();
    let target = root.0.join("manifest");
    for before in [true, false] {
        fs::write(&target, b"old").unwrap();
        let (memory, task) = context(256 * 1024);
        let io = GenerationIo::new(&memory, &task);
        let mut temporary = Temporary::new(&target, &memory, &task).unwrap();
        fs::write(&temporary.path, b"new").unwrap();
        if before {
            task.cancellation().cancel();
        }
        let result = io.replace_with(&mut temporary, &target, |source, target| {
            durable_replace_file(source, target)?;
            task.cancellation().cancel();
            Ok(())
        });
        assert_eq!(result.is_err(), before);
        drop(temporary);
        assert_eq!(
            fs::read(&target).unwrap(),
            if before { b"old" } else { b"new" }
        );
        assert_eq!(fs::read_dir(&root.0).unwrap().count(), 1);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

struct CancelReader<'a> {
    bytes: Cursor<Vec<u8>>,
    task: &'a RuntimeTaskContext,
    reads: usize,
}
impl Read for CancelReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        assert!(output.len() <= SPOOL_BUFFER_BYTES);
        self.reads += 1;
        let count = self.bytes.read(output)?;
        self.task.cancellation().cancel();
        Ok(count)
    }
}

#[test]
fn checksum_and_copy_stop_at_the_next_bounded_io_checkpoint() {
    for copy in [false, true] {
        let task = RuntimeTaskContext::default();
        let mut reader = CancelReader {
            bytes: Cursor::new(vec![1; 8 * SPOOL_BUFFER_BYTES]),
            task: &task,
            reads: 0,
        };
        let result = if copy {
            copy_reader(&mut reader, &mut io::sink(), &task).map(|length| (length, 0))
        } else {
            checksum_reader(&mut reader, &task)
        };
        assert!(result.unwrap_err().to_string().contains("cancel"));
        assert_eq!(reader.reads, 1);
    }
}

#[test]
fn native_path_scratch_is_admitted_before_the_operation() {
    let root = Directory::new();
    let mut path = root.0.clone();
    while path.as_os_str().as_encoded_bytes().len() < 420 {
        path.push("long-component");
    }
    fs::create_dir_all(&path).unwrap();
    let bytes = native_path::bytes(&path).unwrap();
    assert!(bytes > 0);
    for short in [0, 1] {
        let (memory, task) = context(bytes - short);
        let invoked = Cell::new(false);
        let result = GenerationIo::new(&memory, &task).native(&[&path], || invoked.set(true));
        assert_eq!(result.is_ok(), short == 0);
        assert_eq!(invoked.get(), short == 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}
