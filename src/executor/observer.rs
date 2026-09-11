//! Root facade for executor-owned query observation.

pub(super) use skein_executor::observer::{
    blocking_operator_kinds, QueryExecutionObserver, QueryExecutionReports,
};

#[cfg(test)]
#[path = "observer/facade_tests.rs"]
mod tests;
