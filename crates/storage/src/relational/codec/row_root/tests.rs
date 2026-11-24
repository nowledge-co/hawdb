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

//! Row-root reconstruction and checkpoint overflow rebinding keep each read
//! failure's type; only a content or structure mismatch is corruption.

use super::{overflow_error, row_page_error};
use crate::relational::{
    RelationalError, RelationalOverflowPublicationError, RelationalRowPagePublicationError,
};
use hawdb_core::error::{FileDescriptorError, StorageFormatError};
use hawdb_core::{HawDBError, RuntimeCancellationReason};

fn descriptors() -> FileDescriptorError {
    FileDescriptorError::BudgetExceeded {
        requested: 1,
        available: 0,
        limit: 4,
    }
}

fn newer_format() -> StorageFormatError {
    StorageFormatError::UnsupportedVersion {
        format: hawdb_core::RELATIONAL_ROW_PAGE_FORMAT,
        found: 99,
        supported: 6,
    }
}

/// The relational error and the host error each failure must surface as.
fn assert_typed(
    actual: RelationalError,
    expected: &RelationalError,
    host: fn(&HawDBError) -> bool,
) {
    assert_eq!(&actual, expected);
    let surfaced = HawDBError::from_storage_error(actual);
    assert!(host(&surfaced), "{surfaced:?}");
}

#[test]
fn row_page_read_failures_keep_their_type() {
    let stop = RuntimeCancellationReason::DeadlineExceeded;
    assert_typed(
        row_page_error(RelationalRowPagePublicationError::FileDescriptors(
            descriptors(),
        )),
        &RelationalError::FileDescriptors(descriptors()),
        |error| *error == HawDBError::FileDescriptors(descriptors()),
    );
    assert_typed(
        row_page_error(RelationalRowPagePublicationError::UnsupportedFormat(
            newer_format(),
        )),
        &RelationalError::UnsupportedFormat(newer_format()),
        |error| *error == HawDBError::UnsupportedStorageFormat(newer_format()),
    );
    assert_typed(
        row_page_error(RelationalRowPagePublicationError::Stopped(stop)),
        &RelationalError::Stopped(stop),
        |error| {
            *error == HawDBError::RuntimeTaskStopped(RuntimeCancellationReason::DeadlineExceeded)
        },
    );
    assert_eq!(
        row_page_error(RelationalRowPagePublicationError::Admission(
            "budget".into()
        )),
        RelationalError::Admission("budget".into())
    );
    assert_eq!(
        row_page_error(RelationalRowPagePublicationError::Durability("read".into())),
        RelationalError::Durability("read".into())
    );
    assert!(matches!(
        row_page_error(RelationalRowPagePublicationError::PublishOutcomeUnknown(
            Box::new(HawDBError::Storage("lost".into()))
        )),
        RelationalError::Durability(_)
    ));
    // Content or structure that disagrees with the root stays corruption.
    assert_eq!(
        row_page_error(RelationalRowPagePublicationError::Corrupt(
            "checksum".into()
        )),
        RelationalError::Corruption("checksum".into())
    );
    assert!(matches!(
        row_page_error(RelationalRowPagePublicationError::MissingTable("t".into())),
        RelationalError::Corruption(_)
    ));
    assert!(matches!(
        row_page_error(RelationalRowPagePublicationError::StaleGeneration {
            expected_previous: Some(1),
            actual_previous: Some(2),
        }),
        RelationalError::Corruption(_)
    ));
}

#[test]
fn overflow_read_failures_keep_their_type() {
    let stop = RuntimeCancellationReason::Cancelled;
    assert_typed(
        overflow_error(RelationalOverflowPublicationError::FileDescriptors(
            descriptors(),
        )),
        &RelationalError::FileDescriptors(descriptors()),
        |error| *error == HawDBError::FileDescriptors(descriptors()),
    );
    assert_typed(
        overflow_error(RelationalOverflowPublicationError::UnsupportedFormat(
            newer_format(),
        )),
        &RelationalError::UnsupportedFormat(newer_format()),
        |error| *error == HawDBError::UnsupportedStorageFormat(newer_format()),
    );
    assert_typed(
        overflow_error(RelationalOverflowPublicationError::Stopped(stop)),
        &RelationalError::Stopped(stop),
        |error| *error == HawDBError::RuntimeTaskStopped(RuntimeCancellationReason::Cancelled),
    );
    assert_eq!(
        overflow_error(RelationalOverflowPublicationError::Admission(
            "budget".into()
        )),
        RelationalError::Admission("budget".into())
    );
    assert_eq!(
        overflow_error(RelationalOverflowPublicationError::Durability(
            "read".into()
        )),
        RelationalError::Durability("read".into())
    );
    assert!(matches!(
        overflow_error(RelationalOverflowPublicationError::PublishOutcomeUnknown(
            Box::new(HawDBError::Storage("lost".into()))
        )),
        RelationalError::Durability(_)
    ));
    assert_eq!(
        overflow_error(RelationalOverflowPublicationError::Corrupt(
            "checksum".into()
        )),
        RelationalError::Corruption("checksum".into())
    );
    assert!(matches!(
        overflow_error(RelationalOverflowPublicationError::MissingExtent(
            hawdb_integrity::Sha256Digest::from_bytes([7; 32])
        )),
        RelationalError::Corruption(_)
    ));
    assert!(matches!(
        overflow_error(RelationalOverflowPublicationError::StaleGeneration {
            expected_previous: None,
            actual_previous: Some(3),
        }),
        RelationalError::Corruption(_)
    ));
}
