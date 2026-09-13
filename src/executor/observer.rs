//! Root facade for executor-owned query observation.

pub(super) use skein_executor::observer::QueryExecutionObserver;
#[cfg(test)]
pub(super) use skein_executor::observer::QueryExecutionReports;

#[cfg(test)]
#[path = "observer/facade_tests.rs"]
mod tests;
