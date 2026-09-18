use crate::RuntimeCapability;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HawdbError {
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

impl Display for HawdbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HawdbError::Parse(message) => write!(f, "parse error: {message}"),
            HawdbError::Semantic(message) => write!(f, "semantic error: {message}"),
            HawdbError::Storage(message) => write!(f, "storage error: {message}"),
            HawdbError::StorageIntegrity(message) => {
                write!(f, "storage integrity error: {message}")
            }
            HawdbError::Execution(message) => write!(f, "execution error: {message}"),
            HawdbError::AppendSequenceExhausted {
                table,
                watermark,
                requested,
            } => write!(
                f,
                "append commit sequence exhausted for table {table}: watermark={watermark}, requested={requested}"
            ),
            HawdbError::CapabilityUnavailable { capability } => {
                write!(f, "capability unavailable: {}", capability.as_str())
            }
        }
    }
}

impl std::error::Error for HawdbError {}

pub type Result<T> = std::result::Result<T, HawdbError>;

impl From<std::io::Error> for HawdbError {
    fn from(error: std::io::Error) -> Self {
        HawdbError::Storage(error.to_string())
    }
}
