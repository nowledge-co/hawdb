// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Accounted canonical graph relevance; no host projection supplies signals.

use crate::pipeline::runtime_checkpoint;
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_storage::NodeRecord;
use std::cmp::Ordering;
use std::fmt::Write;

const PROPERTIES: &[&str] = &["id", "title", "name", "summary", "content", "body", "text"];

fn tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .filter(|token| !token.is_empty())
}

fn ascii_order(left: &str, right: &str) -> Ordering {
    left.bytes()
        .map(|byte| byte.to_ascii_lowercase())
        .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase()))
}

struct Terms<'a> {
    values: Vec<&'a str>,
    _allocation: QueryMemoryLease,
}

impl<'a> Terms<'a> {
    fn new(
        text: &'a str,
        account: &QueryMemoryAccount,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        let mut count = 0usize;
        for _ in tokens(text) {
            runtime_checkpoint(task)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| HawDBError::Execution("graph term count overflow".into()))?;
        }
        let bytes = count
            .checked_mul(std::mem::size_of::<&str>())
            .ok_or_else(|| HawDBError::Execution("graph term size overflow".into()))?;
        let allocation = account.reserve(bytes)?;
        let mut values = Vec::with_capacity(count);
        for token in tokens(text) {
            runtime_checkpoint(task)?;
            values.push(token);
        }
        values.sort_unstable_by(|left, right| ascii_order(left, right));
        values.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
        runtime_checkpoint(task)?;
        Ok(Self {
            values,
            _allocation: allocation,
        })
    }
}

/// Fixed canonical graph relevance shared by query and legacy retrieval paths.
pub struct GraphSeedScorer<'a> {
    query: &'a str,
    terms: Terms<'a>,
    account: &'a QueryMemoryAccount,
    task: Option<&'a RuntimeTaskContext>,
}

#[derive(Debug, Clone, Copy)]
pub struct GraphSeedRelevance {
    pub score: f64,
    matched_properties: u8,
}

impl GraphSeedRelevance {
    pub fn matched_properties(&self) -> impl Iterator<Item = &'static str> + '_ {
        PROPERTIES
            .iter()
            .enumerate()
            .filter_map(|(index, property)| {
                (self.matched_properties & (1 << index) != 0).then_some(*property)
            })
    }
}

impl<'a> GraphSeedScorer<'a> {
    pub fn new(
        query: &'a str,
        account: &'a QueryMemoryAccount,
        task: Option<&'a RuntimeTaskContext>,
    ) -> Result<Self> {
        Ok(Self {
            query: query.trim(),
            terms: Terms::new(query, account, task)?,
            account,
            task,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.terms.values.is_empty()
    }

    pub fn score(&self, node: &NodeRecord) -> Result<GraphSeedRelevance> {
        let mut relevance = GraphSeedRelevance {
            score: 0.0,
            matched_properties: 0,
        };
        for (property_index, property) in PROPERTIES.iter().enumerate() {
            runtime_checkpoint(self.task)?;
            let Some(value) = node.properties.get(*property) else {
                continue;
            };
            let (text, _text_allocation) = property_text(value, self.account, self.task)?;
            let property_terms = Terms::new(&text, self.account, self.task)?;
            let mut matched = 0usize;
            let mut right = 0usize;
            for left in &self.terms.values {
                runtime_checkpoint(self.task)?;
                while right < property_terms.values.len()
                    && ascii_order(property_terms.values[right], left).is_lt()
                {
                    right += 1;
                    runtime_checkpoint(self.task)?;
                }
                if right < property_terms.values.len()
                    && property_terms.values[right].eq_ignore_ascii_case(left)
                {
                    matched += 1;
                }
            }
            let mut contains = false;
            let mut exact = false;
            if !self.query.is_empty() {
                for (index, window) in text.as_bytes().windows(self.query.len()).enumerate() {
                    if index % 1024 == 0 {
                        runtime_checkpoint(self.task)?;
                    }
                    if window.eq_ignore_ascii_case(self.query.as_bytes()) {
                        contains = true;
                        break;
                    }
                }
                exact = text.eq_ignore_ascii_case(self.query);
            }
            if matched > 0 || exact || contains {
                relevance.matched_properties |= 1 << property_index;
                relevance.score += matched as f64;
                if contains {
                    relevance.score += 2.0;
                }
                if *property == "id" && exact {
                    relevance.score += 8.0;
                }
            }
        }
        Ok(relevance)
    }
}

#[derive(Default)]
struct TextSize(usize);
impl Write for TextSize {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

fn write_property(
    value: &Value,
    output: &mut impl Write,
    task: Option<&RuntimeTaskContext>,
) -> Result<()> {
    runtime_checkpoint(task)?;
    let write_error = |_| HawDBError::Execution("graph property text size overflow".into());
    match value {
        Value::Null => {}
        Value::Bool(value) => write!(output, "{value}").map_err(write_error)?,
        Value::Int(value) => write!(output, "{value}").map_err(write_error)?,
        Value::Float(value) => write!(output, "{value}").map_err(write_error)?,
        Value::String(value) => output.write_str(value).map_err(write_error)?,
        Value::Uuid(value) => write!(output, "{value}").map_err(write_error)?,
        Value::Binary(bytes) => {
            output.write_str("\\x").map_err(write_error)?;
            for byte in bytes {
                runtime_checkpoint(task)?;
                write!(output, "{byte:02x}").map_err(write_error)?;
            }
        }
        Value::List(values) => {
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.write_char(',').map_err(write_error)?;
                }
                write_property(value, output, task)?;
            }
        }
        Value::Map(values) => {
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    output.write_char(',').map_err(write_error)?;
                }
                output.write_str(key).map_err(write_error)?;
                output.write_char(':').map_err(write_error)?;
                write_property(value, output, task)?;
            }
        }
    }
    Ok(())
}

/// Format canonical properties with admission before allocating owned text.
#[doc(hidden)]
pub fn property_text<'a>(
    value: &'a Value,
    account: &QueryMemoryAccount,
    task: Option<&RuntimeTaskContext>,
) -> Result<(std::borrow::Cow<'a, str>, Option<QueryMemoryLease>)> {
    if let Value::String(text) = value {
        return Ok((std::borrow::Cow::Borrowed(text), None));
    }
    let mut size = TextSize::default();
    write_property(value, &mut size, task)?;
    let allocation = account.reserve(size.0)?;
    let mut text = String::with_capacity(size.0);
    write_property(value, &mut text, task)?;
    Ok((std::borrow::Cow::Owned(text), Some(allocation)))
}

/// Read canonical numeric features without allocating property text.
///
/// Singleton lists have the same text as their only value; larger lists, maps,
/// binary values, UUIDs and booleans cannot format as finite numbers. Empty
/// external/source IDs retain the legacy canonical-ID and source-alias fallback.
#[doc(hidden)]
pub fn numeric_property(node: &NodeRecord, key: &str) -> Option<f64> {
    let value = match key {
        "kind" => return None,
        "external_id" => {
            let Some(value) = node
                .properties
                .get("id")
                .filter(|value| !property_text_is_empty(value))
            else {
                return Some(node.id.0 as f64);
            };
            value
        }
        "source_id" => ["source_id", "thread_id", "source"]
            .into_iter()
            .filter_map(|key| node.properties.get(key))
            .find(|value| !property_text_is_empty(value))?,
        _ => node.properties.get(key)?,
    };
    finite_numeric_value(value)
}

fn finite_numeric_value(mut value: &Value) -> Option<f64> {
    while let Value::List(values) = value {
        if values.len() != 1 {
            return None;
        }
        value = &values[0];
    }
    let number = match value {
        Value::Int(value) => *value as f64,
        Value::Float(value) => *value,
        Value::String(value) => value.parse::<f64>().ok()?,
        _ => return None,
    };
    number.is_finite().then_some(number)
}

fn property_text_is_empty(mut value: &Value) -> bool {
    while let Value::List(values) = value {
        if values.len() != 1 {
            return values.is_empty();
        }
        value = &values[0];
    }
    match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        Value::Map(values) => values.is_empty(),
        _ => false,
    }
}
