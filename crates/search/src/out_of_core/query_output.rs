// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Native outputs reserve retained capacity before any result payload allocation.

use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, ACCOUNT_METADATA_BYTES, MAP_ENTRY_BYTES,
};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;
use hawdb_qos::{RuntimeRetainedResult, RuntimeRetainedResultError, RuntimeRetainedResultResource};
use std::fmt::Write as _;
use std::sync::Mutex;

pub(super) mod matches;
pub(super) mod report;
#[cfg(test)]
mod tests;

/// Errors retain shared backpressure metadata separately from query failures.
#[derive(Debug)]
pub enum SearchQueryError {
    Query(HawDBError),
    Retention(RuntimeRetainedResultError),
}
pub type SearchQueryResult<T> = std::result::Result<T, SearchQueryError>;
impl From<HawDBError> for SearchQueryError {
    fn from(error: HawDBError) -> Self {
        Self::Query(error)
    }
}
impl From<ResourceBudgetError> for SearchQueryError {
    fn from(error: ResourceBudgetError) -> Self {
        Self::Query(error.into())
    }
}
impl From<RuntimeRetainedResultError> for SearchQueryError {
    fn from(error: RuntimeRetainedResultError) -> Self {
        Self::Retention(error)
    }
}
impl std::fmt::Display for SearchQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(error) => error.fmt(f),
            Self::Retention(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SearchQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Query(error) => error,
            Self::Retention(error) => error,
        })
    }
}
impl SearchQueryError {
    /// Whether retained-result admission reported retryable shared pressure.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retention(error) if error.is_retryable())
    }
}

/// An immutable native query result. Its retained grant outlives the query,
/// permit and reader. There is no owned extraction or implicit Clone.
/// The caller's result ceiling stays conservatively reserved until destruction.
#[derive(Debug)]
pub struct SearchRetainedOutput {
    output: SearchOutOfCoreOutput,
    // Payload and account metadata close before their runtime reservation.
    _memory: QueryMemoryLease,
    runtime: RuntimeRetainedResult,
}
impl SearchRetainedOutput {
    pub fn result(&self) -> &crate::SearchResultSet {
        &self.output.result
    }
    pub fn metrics(&self) -> &SearchOutOfCoreMetrics {
        &self.output.metrics
    }
    /// Prepaid payload capacity, including retained query-account metadata.
    /// This is an admission ceiling rather than an allocator/RSS measurement.
    pub fn retained_capacity_bytes(&self) -> u64 {
        self.runtime.capacity_bytes()
    }
}

pub(super) struct Input {
    pub(super) options: SearchQueryOptions,
    _memory: QueryMemoryLease,
}
impl Input {
    pub(super) fn new(options: SearchQueryOptions, query: QueryContext<'_>) -> Result<Self> {
        query.checkpoint()?;
        let bytes = options
            .metadata_filters
            .iter()
            .try_fold(0, |bytes, (key, value)| {
                add(
                    bytes,
                    add(MAP_ENTRY_BYTES, add(key.capacity(), value.capacity())?)?,
                )
            })?;
        let memory = query.memory.reserve(bytes)?;
        let input = Self {
            options,
            _memory: memory,
        };
        query.checkpoint()?;
        Ok(input)
    }
}

pub(super) struct Frame<'a> {
    query: QueryContext<'a>,
    memory: Mutex<QueryMemoryLease>,
    runtime: RuntimeRetainedResult,
    limit: usize,
}
impl<'a> Frame<'a> {
    pub(super) fn new(
        query: QueryContext<'a>,
        permit: &hawdb_qos::RuntimePermit,
    ) -> SearchQueryResult<Self> {
        query.checkpoint()?;
        let view = size_of::<SearchRetainedOutput>() as u64;
        let overhead = RuntimeRetainedResult::owner_overhead_bytes()
            .checked_add(RuntimeRetainedResult::handle_overhead_bytes())
            .and_then(|n| n.checked_add(view))
            .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
        let requested = overhead
            .checked_add(ACCOUNT_METADATA_BYTES as u64)
            .ok_or(RuntimeRetainedResultError::SizeOverflow)?;
        let grant = permit.request().result_bytes;
        if grant < requested {
            return Err(RuntimeRetainedResultError::Capacity {
                resource: RuntimeRetainedResultResource::QueryBytes,
                requested,
                available: grant,
                retryable: false,
            }
            .into());
        }
        // The caller's explicit ceiling is prepaid. Every subsequent result
        // allocation also checks this ceiling and the original query ledger.
        let capacity = grant - overhead;
        let runtime = permit.reserve_retained_result(capacity, view)?;
        let limit =
            usize::try_from(capacity).map_err(|_| RuntimeRetainedResultError::SizeOverflow)?;
        let memory = query.memory.reserve(ACCOUNT_METADATA_BYTES)?;
        Ok(Self {
            query,
            memory: Mutex::new(memory),
            runtime,
            limit,
        })
    }
    fn grow(&self, bytes: usize) -> Result<()> {
        self.query.checkpoint()?;
        let mut memory = self
            .memory
            .lock()
            .map_err(|_| HawDBError::Execution("search result memory lock poisoned".into()))?;
        if add(memory.bytes(), bytes)? > self.limit {
            return Err(ResourceBudgetError {
                resource: ResourceBudgetKind::MemoryBytes,
                scope: ResourceBudgetScope::Query,
                owner: "search retained output".into(),
                memory_class: None,
                requested: bytes as u64,
                used: memory.bytes() as u64,
                budget: self.limit as u64,
                budget_option: "RuntimePermit.request.result_bytes",
                message: "search output exceeds admitted retained capacity".into(),
            }
            .into());
        }
        memory.grow(bytes)
    }
    fn shrink(&self, bytes: usize) -> Result<()> {
        self.memory
            .lock()
            .map_err(|_| HawDBError::Execution("search result memory lock poisoned".into()))?
            .shrink(bytes);
        Ok(())
    }
    pub(super) fn finish(self, output: SearchOutOfCoreOutput) -> Result<SearchRetainedOutput> {
        // Establish owning drop order before the final fallible handoff.
        let (memory, poisoned) = match self.memory.into_inner() {
            Ok(memory) => (memory, false),
            Err(error) => (error.into_inner(), true),
        };
        let result = SearchRetainedOutput {
            output,
            _memory: memory,
            runtime: self.runtime,
        };
        if poisoned {
            return Err(HawDBError::Execution(
                "search result memory lock poisoned".into(),
            ));
        }
        self.query.checkpoint()?;
        Ok(result)
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct Memory<'a>(pub(super) Option<&'a Frame<'a>>);
impl Memory<'_> {
    pub(super) fn vec<T>(self, capacity: usize) -> Result<Vec<T>> {
        if let Some(frame) = self.0 {
            frame.grow(mul(capacity, size_of::<T>())?)?;
        }
        let values = Vec::with_capacity(capacity);
        if self.0.is_some() && size_of::<T>() != 0 && values.capacity() != capacity {
            return Err(HawDBError::Execution(
                "output vector exceeded admission".into(),
            ));
        }
        Ok(values)
    }
    pub(super) fn push<T>(self, values: &mut Vec<T>, value: T) -> Result<()> {
        if values.len() == values.capacity()
            && let Some(frame) = self.0
        {
            let capacity = add(values.len(), 1)?.max(values.capacity().saturating_mul(2));
            let mut next = self.vec(capacity)?;
            next.append(values);
            let old = mul(values.capacity(), size_of::<T>())?;
            *values = next;
            frame.shrink(old)?;
        }
        values.push(value);
        Ok(())
    }
    pub(super) fn text(self, value: &str) -> Result<String> {
        if let Some(frame) = self.0 {
            frame.grow(value.len())?;
        }
        let output = value.to_owned();
        if self.0.is_some() && output.capacity() != value.len() {
            return Err(HawDBError::Execution(
                "output string exceeded admission".into(),
            ));
        }
        Ok(output)
    }
    pub(super) fn one_owned<T>(self, value: T) -> Result<Vec<T>> {
        let mut output = self.vec(1)?;
        output.push(value);
        Ok(output)
    }
    pub(super) fn one_text(self, value: &str) -> Result<Vec<String>> {
        self.one_owned(self.text(value)?)
    }
    pub(super) fn optional(self, value: Option<&str>) -> Result<Option<String>> {
        value.map(|value| self.text(value)).transpose()
    }
    pub(super) fn codes<T: Copy>(self, values: &[T]) -> Result<Vec<T>> {
        if self.0.is_none() {
            return Ok(values.to_vec());
        }
        let mut output = self.vec(values.len())?;
        output.extend_from_slice(values);
        Ok(output)
    }
    pub(super) fn strings(self, values: &[String]) -> Result<Vec<String>> {
        if self.0.is_none() {
            return Ok(values.to_vec());
        }
        let mut output = self.vec(values.len())?;
        for value in values {
            output.push(self.text(value)?);
        }
        Ok(output)
    }
    pub(super) fn combine<T: Copy>(self, left: &[T], right: &[T]) -> Result<Vec<T>> {
        let mut output = self.vec(add(left.len(), right.len())?)?;
        output.extend_from_slice(left);
        output.extend_from_slice(right);
        Ok(output)
    }
    pub(super) fn combine_strings(self, left: &[String], right: &[String]) -> Result<Vec<String>> {
        let mut output = self.vec(add(left.len(), right.len())?)?;
        for value in left.iter().chain(right) {
            output.push(self.text(value)?);
        }
        Ok(output)
    }
    pub(super) fn map(self, values: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
        if self.0.is_none() {
            return Ok(values.clone());
        }
        let mut output = BTreeMap::new();
        for (key, value) in values {
            if let Some(frame) = self.0 {
                frame.grow(MAP_ENTRY_BYTES)?;
            }
            output.insert(self.text(key)?, self.text(value)?);
        }
        Ok(output)
    }
    pub(super) fn format(self, arguments: std::fmt::Arguments<'_>) -> Result<String> {
        if self.0.is_none() {
            return Ok(std::fmt::format(arguments));
        }
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(s.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        count
            .write_fmt(arguments)
            .map_err(|_| HawDBError::Execution("output format size overflow".into()))?;
        if let Some(frame) = self.0 {
            frame.grow(count.0)?;
        }
        let mut output = String::with_capacity(count.0);
        output
            .write_fmt(arguments)
            .map_err(|_| HawDBError::Execution("output format failed".into()))?;
        if output.capacity() != count.0 {
            return Err(HawDBError::Execution(
                "output format exceeded admission".into(),
            ));
        }
        Ok(output)
    }
}
