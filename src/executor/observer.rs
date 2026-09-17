//! Root facade for executor-owned query observation.

pub(super) use skein_executor::observer::QueryExecutionObserver;

#[cfg(test)]
#[path = "observer/facade_tests.rs"]
mod tests;
