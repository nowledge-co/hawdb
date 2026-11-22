// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn execute(db: *mut HawdbDatabase, sql: bool) -> bool {
    let text = if sql {
        "SELECT 1 AS value"
    } else {
        "RETURN 1 AS value"
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
        if ok {
            let value: serde_json::Value = serde_json::from_slice(std::slice::from_raw_parts(
                result.ptr.cast::<u8>(),
                result.len,
            ))
            .unwrap();
            assert_eq!(value["columns"][0], "value");
            assert_eq!(value["rows"][0][0], 1);
        }
        hawdb_buffer_free(&mut result);
        hawdb_buffer_free(&mut error);
        ok
    }
}

#[test]
fn ordinary_ffi_reads_bypass_database_owner_and_overlap() {
    let database = Box::into_raw(Box::new(
        HawdbDatabase::new(HawDBEmbedded::open_in_memory()),
    ));
    assert!(execute(database, false));
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
    assert!(first.unwrap());
    assert!(second.unwrap());
}
