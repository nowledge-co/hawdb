//! Compatibility facade for executor-owned numeric execution.

pub(super) use skein_executor::numeric::{
    default_morsel_parallelism, supports_parallel_morsel_execution,
};

#[cfg(test)]
use super::*;

#[cfg(test)]
mod scan_error_tests;
