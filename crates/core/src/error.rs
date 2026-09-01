use crate::RuntimeCapability;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkeinError {
    Parse(String),
    Semantic(String),
    Storage(String),
    StorageIntegrity(String),
    Execution(String),
    AppendSequenceExhausted {
        table: String,
        watermark: i64,
        requested: usize,
    },
    CapabilityUnavailable {
        capability: RuntimeCapability,
    },
}

impl Display for SkeinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SkeinError::Parse(message) => write!(f, "parse error: {message}"),
            SkeinError::Semantic(message) => write!(f, "semantic error: {message}"),
            SkeinError::Storage(message) => write!(f, "storage error: {message}"),
            SkeinError::StorageIntegrity(message) => {
                write!(f, "storage integrity error: {message}")
            }
            SkeinError::Execution(message) => write!(f, "execution error: {message}"),
            SkeinError::AppendSequenceExhausted {
                table,
                watermark,
                requested,
            } => write!(
                f,
                "append commit sequence exhausted for table {table}: watermark={watermark}, requested={requested}"
            ),
            SkeinError::CapabilityUnavailable { capability } => {
                write!(f, "capability unavailable: {}", capability.as_str())
            }
        }
    }
}

impl std::error::Error for SkeinError {}

pub type Result<T> = std::result::Result<T, SkeinError>;

impl From<std::io::Error> for SkeinError {
    fn from(error: std::io::Error) -> Self {
        SkeinError::Storage(error.to_string())
    }
}
