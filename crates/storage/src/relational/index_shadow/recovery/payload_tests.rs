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
use crate::relational::index_shadow::demand_read::{IndexReadObserver, IndexReadPreflightError};
use std::cell::Cell;
use std::rc::Rc;

struct OwnedPayload(PathBuf);

impl OwnedPayload {
    fn new(bytes: &[u8]) -> Self {
        static NEXT_PAYLOAD: AtomicU64 = AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "hawdb-recovery-payload-{}-{nonce}-{}",
            std::process::id(),
            NEXT_PAYLOAD.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let fixture = Self(directory);
        std::fs::write(fixture.path(), bytes).unwrap();
        fixture
    }

    fn path(&self) -> PathBuf {
        self.0.join("delta.hawdb")
    }
}

impl Drop for OwnedPayload {
    fn drop(&mut self) {
        std::fs::remove_file(self.path()).unwrap();
        std::fs::remove_dir(&self.0).unwrap();
    }
}

struct CountingFile {
    file: File,
    path: PathBuf,
    resize_on_read: Option<u64>,
    read_calls: usize,
    bytes_read: usize,
    largest_request: usize,
    admitted_bytes: Option<Rc<Cell<usize>>>,
}

impl CountingFile {
    fn open(fixture: &OwnedPayload, resize_on_read: Option<u64>) -> Self {
        Self {
            file: File::open(fixture.path()).unwrap(),
            path: fixture.path(),
            resize_on_read,
            read_calls: 0,
            bytes_read: 0,
            largest_request: 0,
            admitted_bytes: None,
        }
    }

    fn read_payload(&mut self, encoded_len: usize) -> Result<Vec<u8>, RelationalIndexShadowError> {
        self.read_payload_admitted(encoded_len, IndexReadAdmission::default())
    }

    fn read_payload_admitted(
        &mut self,
        encoded_len: usize,
        admission: IndexReadAdmission<'_>,
    ) -> Result<Vec<u8>, RelationalIndexShadowError> {
        read_recovery_delta_payload(
            self,
            encoded_len,
            |reader| reader.file.metadata().map(|metadata| metadata.len()),
            admission,
        )
    }
}

impl Read for CountingFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if let Some(admitted) = &self.admitted_bytes {
            assert!(admitted.get() >= self.bytes_read + buffer.len());
        }
        if let Some(length) = self.resize_on_read.take() {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&self.path)?
                .set_len(length)?;
        }
        self.read_calls += 1;
        self.largest_request = self.largest_request.max(buffer.len());
        let bytes_read = self.file.read(buffer)?;
        self.bytes_read += bytes_read;
        Ok(bytes_read)
    }
}

struct FileAdmission {
    bytes: Rc<Cell<usize>>,
    reject: bool,
}

impl IndexReadObserver for FileAdmission {
    fn check_charge(&self, _charge: IndexReadCharge) -> Result<(), IndexReadPreflightError> {
        if self.reject {
            Err(IndexReadPreflightError::Budget)
        } else {
            Ok(())
        }
    }

    fn set_budget_refusal(&self, _refused: bool) {}

    fn charge(&self, charge: IndexReadCharge) -> Result<(), RelationalIndexShadowError> {
        self.check_charge(charge)
            .map_err(IndexReadPreflightError::into_error)?;
        let IndexReadCharge::FileBytes(bytes) = charge else {
            panic!("payload admission must charge file bytes")
        };
        self.bytes.set(self.bytes.get() + bytes);
        Ok(())
    }

    fn file_budget(&self, requested: usize) -> Result<usize, RelationalIndexShadowError> {
        Ok(requested)
    }
}

#[test]
fn recovery_payload_charges_file_bytes_before_io_and_keeps_failed_read_charges() {
    for resize in [None, Some(255)] {
        let fixture = OwnedPayload::new(&vec![0x39; 256]);
        let mut reader = CountingFile::open(&fixture, resize);
        let bytes = Rc::new(Cell::new(0));
        reader.admitted_bytes = Some(Rc::clone(&bytes));
        let observer = FileAdmission {
            bytes: Rc::clone(&bytes),
            reject: false,
        };
        let result = reader.read_payload_admitted(256, IndexReadAdmission::new(&observer));
        assert_eq!(result.is_ok(), resize.is_none());
        assert_eq!(bytes.get(), 256);
        assert_eq!(reader.bytes_read, resize.unwrap_or(256) as usize);
    }
}

#[test]
fn recovery_payload_admission_refusal_precedes_io() {
    let fixture = OwnedPayload::new(&vec![0x39; 256]);
    let mut reader = CountingFile::open(&fixture, None);
    let observer = FileAdmission {
        bytes: Rc::new(Cell::new(0)),
        reject: true,
    };
    assert!(matches!(
        reader.read_payload_admitted(256, IndexReadAdmission::new(&observer)),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert_eq!(reader.read_calls, 0);
    assert_eq!(observer.bytes.get(), 0);
}

#[test]
fn recovery_payload_matching_file_reads_exact_admitted_bytes() {
    let expected = vec![0x39; 256];
    let fixture = OwnedPayload::new(&expected);
    let mut reader = CountingFile::open(&fixture, None);

    assert_eq!(reader.read_payload(expected.len()).unwrap(), expected);
    assert!(reader.read_calls > 0);
    assert_eq!(reader.bytes_read, expected.len());
    assert_eq!(reader.largest_request, expected.len());
}

#[test]
fn recovery_payload_length_drift_rejects_before_payload_io() {
    let admitted_bytes = 256;
    for actual_bytes in [admitted_bytes - 1, admitted_bytes + 1] {
        let fixture = OwnedPayload::new(&vec![0x39; actual_bytes]);
        let mut reader = CountingFile::open(&fixture, None);

        assert!(matches!(
            reader.read_payload(admitted_bytes),
            Err(RelationalIndexShadowError::Corrupt(_))
        ));
        assert_eq!(reader.read_calls, 0);
        assert_eq!(reader.bytes_read, 0);
    }
}

#[test]
fn recovery_payload_concurrent_resize_stays_within_admitted_bytes() {
    let admitted_bytes = 256;
    for actual_bytes in [admitted_bytes - 1, admitted_bytes + 1] {
        let fixture = OwnedPayload::new(&vec![0x39; admitted_bytes]);
        // Resize the actual file after the initial handle-length check, at the
        // first real read, to cover both growth and an UnexpectedEof shrink.
        let mut reader = CountingFile::open(&fixture, Some(actual_bytes as u64));

        assert!(matches!(
            reader.read_payload(admitted_bytes),
            Err(RelationalIndexShadowError::Corrupt(_))
        ));
        assert!(reader.read_calls > 0);
        assert_eq!(reader.bytes_read, actual_bytes.min(admitted_bytes));
        assert_eq!(reader.largest_request, admitted_bytes);
    }
}
