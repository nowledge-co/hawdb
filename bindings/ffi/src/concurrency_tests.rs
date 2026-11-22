// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn execute(db: *mut HawdbDatabase, sql: bool) -> Result<(), String> {
    let text = if sql {
        "SELECT 1 AS value"
    } else {
        "MATCH (n:Item) RETURN n.value AS value LIMIT 1"
    };
    let mut result = HawdbBuffer {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let mut error = HawdbBuffer {
        ptr: ptr::null_mut(),
        len: 0,
    };
    unsafe {
        let ok = query_impl(
            db,
            FfiStr {
                ptr: text.as_ptr().cast(),
                len: text.len(),
            },
            FfiStr {
                ptr: ptr::null(),
                len: 0,
            },
            sql,
            false,
            &mut result,
            &mut error,
        );
        let outcome = if ok {
            let value: serde_json::Value = serde_json::from_slice(std::slice::from_raw_parts(
                result.ptr.cast::<u8>(),
                result.len,
            ))
            .unwrap();
            assert_eq!(value["columns"][0], "value");
            assert_eq!(value["rows"][0][0], 1);
            Ok(())
        } else if error.ptr.is_null() {
            Err("FFI query failed without an error buffer".to_string())
        } else {
            Err(String::from_utf8_lossy(std::slice::from_raw_parts(
                error.ptr.cast::<u8>(),
                error.len,
            ))
            .into_owned())
        };
        hawdb_buffer_free(&mut result);
        hawdb_buffer_free(&mut error);
        outcome
    }
}

#[test]
fn ordinary_ffi_reads_bypass_database_owner_and_overlap() {
    let mut embedded = HawDBEmbedded::open_in_memory();
    embedded
        .query_admitted("CREATE (:Item {value: 1})")
        .unwrap();
    let database = Box::into_raw(Box::new(HawdbDatabase::new(embedded)));
    execute(database, false).unwrap();
    let address = database as usize;
    let (held, acquired) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let database = unsafe { &*(address as *const HawdbDatabase) };
        database.inner.with_embedded_mut(|_| {
            held.send(()).unwrap();
            resume.recv().unwrap();
        });
    });
    acquired.recv_timeout(Duration::from_secs(10)).unwrap();
    let (done, completed) = mpsc::channel();
    let mut readers = Vec::new();
    for sql in [false, true] {
        let done = done.clone();
        readers.push(std::thread::spawn(move || {
            done.send(execute(address as *mut HawdbDatabase, sql))
                .unwrap()
        }));
    }
    drop(done);
    let first = completed.recv_timeout(Duration::from_secs(10));
    let second = completed.recv_timeout(Duration::from_secs(10));
    release.send(()).unwrap();
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }
    unsafe { hawdb_close(database) };
    first.unwrap().unwrap();
    second.unwrap().unwrap();
}
