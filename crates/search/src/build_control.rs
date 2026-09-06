//! Cooperative controls for one mutable projection build, never for its readers.

use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;

pub(crate) fn checkpoint(context: &RuntimeTaskContext) -> Result<()> {
    context
        .checkpoint()
        .map_err(|reason| SkeinError::Execution(format!("search generation build {reason}")))
}
