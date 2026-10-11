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

use hawdb::{EmbeddedQueryError, HawDBError};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    hawdb.exceptions,
    Error,
    PyException,
    "Base error raised by HawDB."
);
create_exception!(
    hawdb.exceptions,
    ParseError,
    Error,
    "Statement failed to parse."
);
create_exception!(
    hawdb.exceptions,
    SemanticError,
    Error,
    "Statement failed semantic checks."
);
create_exception!(
    hawdb.exceptions,
    StorageError,
    Error,
    "Storage layer failure."
);
create_exception!(
    hawdb.exceptions,
    IntegrityError,
    StorageError,
    "Storage integrity violation."
);
create_exception!(
    hawdb.exceptions,
    DescriptorError,
    StorageError,
    "File descriptor budget or storage descriptor failure."
);
create_exception!(
    hawdb.exceptions,
    ExecutionError,
    Error,
    "Statement failed during execution."
);
create_exception!(
    hawdb.exceptions,
    ConflictError,
    Error,
    "Transaction conflict or exhausted commit sequence."
);
create_exception!(
    hawdb.exceptions,
    CapabilityError,
    Error,
    "A required runtime capability is unavailable."
);
create_exception!(
    hawdb.exceptions,
    BranchError,
    Error,
    "Branch command is unsupported or the branch is busy."
);
create_exception!(
    hawdb.exceptions,
    AdmissionError,
    Error,
    "Query admission was rejected by runtime resource governance."
);
create_exception!(
    hawdb.exceptions,
    TaskStoppedError,
    Error,
    "The runtime task was stopped before producing a result."
);
create_exception!(
    hawdb.exceptions,
    RetainedError,
    Error,
    "Experimental retained-interface failure; kind and retryable preserve the outcome."
);
create_exception!(
    hawdb.exceptions,
    BackpressureError,
    RetainedError,
    "Release held views and retry; the source has not advanced."
);

/// Maps a [`HawDBError`] onto the matching Python exception.
pub fn format_hawdb_error(error: &HawDBError) -> PyErr {
    let message = error.to_string();
    match error {
        HawDBError::Parse(_) => ParseError::new_err(message),
        HawDBError::Semantic(_) => SemanticError::new_err(message),
        HawDBError::Storage(_) => StorageError::new_err(message),
        HawDBError::FileDescriptors(_) => DescriptorError::new_err(message),
        HawDBError::StorageIntegrity(_) => IntegrityError::new_err(message),
        HawDBError::Execution(_)
        | HawDBError::ReadBudgetExceeded(_)
        | HawDBError::GraphExpansionCandidateLimitExceeded { .. }
        | HawDBError::GraphExpansionPayloadLimitExceeded { .. } => ExecutionError::new_err(message),
        HawDBError::TransactionConflict { .. } | HawDBError::AppendSequenceExhausted { .. } => {
            ConflictError::new_err(message)
        }
        HawDBError::CapabilityUnavailable { .. } => CapabilityError::new_err(message),
        HawDBError::BranchCommandUnsupported { .. } | HawDBError::BranchBusy { .. } => {
            BranchError::new_err(message)
        }
    }
}

/// Maps an [`EmbeddedQueryError`] onto the matching Python exception.
pub fn format_embedded_error(error: &EmbeddedQueryError) -> PyErr {
    match error {
        EmbeddedQueryError::Database(inner) => format_hawdb_error(inner),
        EmbeddedQueryError::Admission(_) => AdmissionError::new_err(error.to_string()),
        EmbeddedQueryError::Stopped(_) => TaskStoppedError::new_err(error.to_string()),
    }
}

/// Adds every exception type to the `hawdb.exceptions` module.
pub fn register_exceptions(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("Error", py.get_type::<Error>())?;
    module.add("ParseError", py.get_type::<ParseError>())?;
    module.add("SemanticError", py.get_type::<SemanticError>())?;
    module.add("StorageError", py.get_type::<StorageError>())?;
    module.add("IntegrityError", py.get_type::<IntegrityError>())?;
    module.add("DescriptorError", py.get_type::<DescriptorError>())?;
    module.add("ExecutionError", py.get_type::<ExecutionError>())?;
    module.add("ConflictError", py.get_type::<ConflictError>())?;
    module.add("CapabilityError", py.get_type::<CapabilityError>())?;
    module.add("BranchError", py.get_type::<BranchError>())?;
    module.add("AdmissionError", py.get_type::<AdmissionError>())?;
    module.add("TaskStoppedError", py.get_type::<TaskStoppedError>())?;
    module.add("RetainedError", py.get_type::<RetainedError>())?;
    module.add("BackpressureError", py.get_type::<BackpressureError>())?;
    Ok(())
}
