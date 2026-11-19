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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeAdmissionRefusal {
    pub code: &'static str,
    pub requested: u64,
    pub available: u64,
    pub retryable: bool,
}

impl Display for RuntimeAdmissionRefusal {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "runtime admission {}: requested {}, available {}",
            self.code, self.requested, self.available
        )
    }
}

impl std::error::Error for RuntimeAdmissionRefusal {}
