// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A runtime governor admission refusal carried through `HawDBError`.

use std::fmt::{Display, Formatter};

/// The runtime governor refused to admit work or to fund retained memory.
///
/// It mirrors the governor's `RuntimeAdmissionError`: `code` is its stable
/// code (`memory_saturated`, `io_saturated`, `memory_pressure`, ...), and
/// `requested`/`available` are in the code's units. Nothing was executed or
/// written. This is not a storage failure and never poisons a handle. When
/// `retryable` is true the refusal reflects transient load (other work holds
/// the capacity, or the host's free memory is low): drop the refused request
/// and retry it later unchanged. When false the request can never fit the
/// configured limits and must be made smaller.
///
/// `waited` is `None` for an immediate refusal. It is the time spent queued
/// when the caller chose to wait for admission (see
/// `RuntimeTaskContext::with_admission_wait`) and the refusal came from the
/// queue: a retryable refusal with `waited` set means the wait deadline
/// expired while queued, and `code` is the last resource still short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeAdmissionRefusal {
    pub code: &'static str,
    pub requested: u64,
    pub available: u64,
    pub retryable: bool,
    pub waited: Option<crate::time::Duration>,
}

impl Display for RuntimeAdmissionRefusal {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "runtime admission {}: requested {}, available {}",
            self.code, self.requested, self.available
        )?;
        match self.waited {
            Some(waited) => write!(formatter, ", after {} ms queued", waited.as_millis()),
            None => Ok(()),
        }
    }
}

impl std::error::Error for RuntimeAdmissionRefusal {}
