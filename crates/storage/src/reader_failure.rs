// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Reader refusal classification before physical-failure poison is latched.

use crate::background::CheckpointWorkError;
use crate::cache::SegmentCacheError;
use crate::graph_descriptor_page::GraphDescriptorPageError;
use crate::graph_descriptor_tree::GraphDescriptorTreeError;
use crate::scan::SegmentReadError;
use hawdb_core::{
    ReadBudgetError, ResourceBudgetError, RuntimeCancellationReason, RuntimeIoWaveError,
    RuntimeMemoryError,
};
use std::error::Error;

pub(crate) fn is_admission_or_cancellation(error: &(dyn Error + 'static)) -> bool {
    if hawdb_core::error::file_descriptor_error(error).is_some() {
        return true;
    }
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<ResourceBudgetError>()
            || error.is::<ReadBudgetError>()
            || error.is::<RuntimeCancellationReason>()
            || error.is::<RuntimeMemoryError>()
            || error.is::<RuntimeIoWaveError>()
            || error.is::<hawdb_qos::RuntimeAdmissionError>()
            || error.is::<CheckpointWorkError>()
            || matches!(
                error.downcast_ref::<SegmentReadError>(),
                Some(
                    SegmentReadError::WaveBudgetExceeded { .. }
                        | SegmentReadError::RangeTooLarge { .. }
                )
            )
            || matches!(
                error.downcast_ref::<SegmentCacheError>(),
                Some(
                    SegmentCacheError::EntryTooLarge { .. }
                        | SegmentCacheError::PinnedCapacity { .. }
                )
            )
            || matches!(
                error.downcast_ref::<GraphDescriptorTreeError>(),
                Some(GraphDescriptorTreeError::Admission(_))
            )
            || matches!(
                error.downcast_ref::<GraphDescriptorPageError>(),
                Some(GraphDescriptorPageError::Admission(_))
            )
        {
            return true;
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            if matches!(
                error.kind(),
                std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::OutOfMemory
            ) {
                return true;
            }
            // io::Error::source can skip its immediate boxed cause.
            if let Some(inner) = error.get_ref()
                && is_admission_or_cancellation(inner)
            {
                return true;
            }
        }
        current = error.source();
    }
    false
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use hawdb_core::{ResourceBudgetKind, ResourceBudgetScope};
    use std::io;

    pub(crate) fn refusals() -> Vec<io::Error> {
        let causes: Vec<Box<dyn Error + Send + Sync>> = vec![
            Box::new(hawdb_core::error::FileDescriptorError::BudgetExceeded {
                requested: 1,
                available: 0,
                limit: 1,
            }),
            Box::new(ResourceBudgetError {
                resource: ResourceBudgetKind::MemoryBytes,
                scope: ResourceBudgetScope::Account,
                owner: "reader".into(),
                memory_class: None,
                requested: 8,
                used: 8,
                budget: 8,
                budget_option: "reader budget",
                message: "reader refused".into(),
            }),
            Box::new(RuntimeCancellationReason::Cancelled),
            Box::new(RuntimeCancellationReason::DeadlineExceeded),
            Box::new(RuntimeMemoryError::Pressure),
            Box::new(RuntimeIoWaveError::ReservationExceeded {
                requested_slots: 2,
                reserved_slots: 1,
            }),
            Box::new(CheckpointWorkError::Contended("reader")),
            Box::new(GraphDescriptorTreeError::Admission("page budget".into())),
            Box::new(GraphDescriptorPageError::Admission("page bytes".into())),
            Box::new(SegmentCacheError::EntryTooLarge {
                entry_bytes: 2,
                capacity_bytes: 1,
            }),
            Box::new(SegmentReadError::WaveBudgetExceeded {
                wave_index: 0,
                scheduled_bytes: 2,
                max_wave_bytes: 1,
            }),
        ];
        causes
            .into_iter()
            .map(|error| io::Error::other(io::Error::other(error)))
            .collect()
    }

    #[test]
    fn nested_refusals_remain_distinct_from_physical_read_failures() {
        for error in refusals() {
            assert!(is_admission_or_cancellation(&error), "{error:?}");
        }
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::OutOfMemory,
        ] {
            assert!(is_admission_or_cancellation(&io::Error::from(kind)));
        }
        for kind in [
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::InvalidData,
            io::ErrorKind::PermissionDenied,
        ] {
            let error = io::Error::other(io::Error::from(kind));
            assert!(!is_admission_or_cancellation(&error), "{error:?}");
        }
        assert!(!is_admission_or_cancellation(
            &GraphDescriptorTreeError::Corrupt("damaged page".into())
        ));
        assert!(!is_admission_or_cancellation(
            &SegmentCacheError::DigestMismatch {
                expected: crate::cache::ContentDigest(1),
                actual: crate::cache::ContentDigest(2),
            }
        ));
    }
}
