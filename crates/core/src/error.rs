use crate::RuntimeCapability;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HawDBError {
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

impl Display for HawDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HawDBError::Parse(message) => write!(f, "parse error: {message}"),
            HawDBError::Semantic(message) => write!(f, "semantic error: {message}"),
            HawDBError::Storage(message) => write!(f, "storage error: {message}"),
            HawDBError::StorageIntegrity(message) => {
                write!(f, "storage integrity error: {message}")
            }
            HawDBError::Execution(message) => write!(f, "execution error: {message}"),
            HawDBError::AppendSequenceExhausted {
                table,
                watermark,
                requested,
            } => write!(
                f,
                "append commit sequence exhausted for table {table}: watermark={watermark}, requested={requested}"
            ),
            HawDBError::CapabilityUnavailable { capability } => {
                write!(f, "capability unavailable: {}", capability.as_str())
            }
        }
    }
}

impl std::error::Error for HawDBError {}

pub type Result<T> = std::result::Result<T, HawDBError>;

impl From<std::io::Error> for HawDBError {
    fn from(error: std::io::Error) -> Self {
        HawDBError::Storage(error.to_string())
    }
}
