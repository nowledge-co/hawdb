// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Canonical metadata filtering with the retrieval pipeline's read budget.

use super::*;
use hawdb_executor::graph_seed::property_text;
use hawdb_executor::store::{AdjacencyReadMemory, ScanControl};
use hawdb_executor::traversal::{visit_one_hop_relationships_with_budget, OneHopRelationshipSpec};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};

pub(super) struct PreparedKnowledgeFilter {
    predicates: SearchPredicateSet,
    _allocation: QueryMemoryLease,
}

impl PreparedKnowledgeFilter {
    pub(super) fn new(
        filters: &BTreeMap<String, String>,
        account: &QueryMemoryAccount,
    ) -> Result<Self> {
        // Before parsing, cover geometric predicate/list capacity, decoded JSON
        // text and the overlapping normalized/enum copies. A JSON list cannot
        // contain more strings than half its quote characters; escaped quotes
        // only increase this conservative upper bound. Decoding and ASCII
        // normalization never enlarge its input bytes, apart from bool aliases.
        let mut bytes = if filters.is_empty() {
            0
        } else {
            filters
                .len()
                .max(4)
                .checked_mul(2 * std::mem::size_of::<SearchPredicate>())
                .ok_or_else(|| HawDBError::Execution("knowledge filter size overflow".into()))?
        };
        for (key, value) in filters {
            let items = (value.bytes().filter(|byte| *byte == b'"').count() / 2 + 1).max(4);
            let text = key
                .len()
                .checked_add(value.len())
                .and_then(|bytes| bytes.checked_add(5))
                .and_then(|bytes| bytes.checked_mul(8));
            let slots = items.checked_mul(8 * std::mem::size_of::<String>());
            bytes = text
                .and_then(|text| slots.and_then(|slots| text.checked_add(slots)))
                .and_then(|extra| bytes.checked_add(extra))
                .ok_or_else(|| HawDBError::Execution("knowledge filter size overflow".into()))?;
        }
        let allocation = account.reserve(bytes)?;
        let predicates = SearchPredicateSet::from_metadata_filters(filters)
            .unwrap_or_else(|_| SearchPredicateSet::unsatisfiable());
        Ok(Self {
            predicates,
            _allocation: allocation,
        })
    }

    pub(super) fn matches(
        &self,
        catalog: &Catalog,
        store: &impl crate::executor::ExecutionStore,
        node: &NodeRecord,
        account: &QueryMemoryAccount,
        budget_bytes: usize,
    ) -> Result<bool> {
        if self.predicates.is_unsatisfiable() {
            return Ok(false);
        }
        for predicate in self.predicates.predicates() {
            let key = predicate.field().name();
            if matches!(
                predicate.op(),
                SearchPredicateOp::Gt(_)
                    | SearchPredicateOp::Gte(_)
                    | SearchPredicateOp::Lt(_)
                    | SearchPredicateOp::Lte(_)
            ) {
                let actual = knowledge_graph_seed_filter_numeric_value(node, key);
                let matches = match (actual, predicate.op()) {
                    (Some(actual), SearchPredicateOp::Gt(expected)) => {
                        parse_metadata_filter_number(expected.as_str())
                            .is_some_and(|expected| actual > expected)
                    }
                    (Some(actual), SearchPredicateOp::Gte(expected)) => {
                        parse_metadata_filter_number(expected.as_str())
                            .is_some_and(|expected| actual >= expected)
                    }
                    (Some(actual), SearchPredicateOp::Lt(expected)) => {
                        parse_metadata_filter_number(expected.as_str())
                            .is_some_and(|expected| actual < expected)
                    }
                    (Some(actual), SearchPredicateOp::Lte(expected)) => {
                        parse_metadata_filter_number(expected.as_str())
                            .is_some_and(|expected| actual <= expected)
                    }
                    _ => false,
                };
                if !matches {
                    return Ok(false);
                }
                continue;
            }
            let values = filter_values(catalog, store, node, key, account, budget_bytes)?;
            let matches = match predicate.op() {
                SearchPredicateOp::Eq(expected) => any_matches(
                    key,
                    &values.values,
                    std::iter::once(expected.as_str()),
                    account,
                )?,
                SearchPredicateOp::In(expected) => any_matches(
                    key,
                    &values.values,
                    expected.iter().map(|expected| expected.as_str()),
                    account,
                )?,
                SearchPredicateOp::NotIn(expected) => !any_matches(
                    key,
                    &values.values,
                    expected.iter().map(|expected| expected.as_str()),
                    account,
                )?,
                SearchPredicateOp::Exists => !values.values.is_empty(),
                SearchPredicateOp::IsMissing => values.values.is_empty(),
                _ => false,
            };
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn any_matches<'a>(
    key: &str,
    actual: &[String],
    expected: impl Iterator<Item = &'a str> + Clone,
    account: &QueryMemoryAccount,
) -> Result<bool> {
    for actual in actual {
        for expected in expected.clone() {
            // Contextual final sigma has the same UTF-8 length as simple sigma.
            // Count character mappings only to bound byte capacity, then keep
            // the legacy whole-string normalization as the comparison oracle.
            let mut bytes = 0usize;
            if key != "kind" && (key == "space_id" || !search_field_is_enum_like(key)) {
                for text in [actual.as_str(), expected] {
                    let mut mapped_bytes = 0usize;
                    for character in text.chars().flat_map(char::to_lowercase) {
                        mapped_bytes =
                            mapped_bytes
                                .checked_add(character.len_utf8())
                                .ok_or_else(|| {
                                    HawDBError::Execution(
                                        "knowledge normalization size overflow".into(),
                                    )
                                })?;
                    }
                    bytes = text
                        .len()
                        .max(mapped_bytes)
                        .checked_mul(2)
                        .and_then(|extra| bytes.checked_add(extra))
                        .ok_or_else(|| {
                            HawDBError::Execution("knowledge normalization size overflow".into())
                        })?;
                }
            }
            let _allocation = account.reserve(bytes)?;
            if knowledge_graph_seed_filter_value_matches(key, actual, expected) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct FilterValues {
    values: Vec<String>,
    allocation: QueryMemoryLease,
}

impl FilterValues {
    fn new(account: &QueryMemoryAccount) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            allocation: account.reserve(0)?,
        })
    }
    fn push(&mut self, text: &str) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .map(|capacity| capacity.max(4))
                .ok_or_else(|| {
                    HawDBError::Execution("knowledge filter capacity overflow".into())
                })?;
            let bytes = (capacity - self.values.capacity())
                .checked_mul(std::mem::size_of::<String>())
                .ok_or_else(|| {
                    HawDBError::Execution("knowledge filter capacity overflow".into())
                })?;
            self.allocation.grow(bytes)?;
            self.values.reserve_exact(capacity - self.values.len());
        }
        self.allocation.grow(text.len())?;
        self.values.push(text.to_string());
        Ok(())
    }
    fn property(
        &mut self,
        value: &Value,
        account: &QueryMemoryAccount,
        skip_empty: bool,
    ) -> Result<bool> {
        let (text, _allocation) = property_text(value, account, None)?;
        if skip_empty && text.is_empty() {
            return Ok(false);
        }
        self.push(&text)?;
        Ok(true)
    }
}

fn filter_values(
    catalog: &Catalog,
    store: &impl crate::executor::ExecutionStore,
    node: &NodeRecord,
    key: &str,
    account: &QueryMemoryAccount,
    budget_bytes: usize,
) -> Result<FilterValues> {
    let mut values = FilterValues::new(account)?;
    match key {
        "kind" => {
            if let Some(kind) = node
                .labels
                .iter()
                .find_map(|id| catalog.label_name(*id).and_then(search_label_to_kind))
            {
                values.push(kind)?;
            }
        }
        "external_id" => {
            if !node
                .properties
                .get("id")
                .map(|value| values.property(value, account, true))
                .transpose()?
                .unwrap_or(false)
            {
                let _allocation = account.reserve(20)?;
                values.push(&node.id.0.to_string())?;
            }
        }
        "source_id" => {
            for key in ["source_id", "thread_id", "source"] {
                if let Some(value) = node.properties.get(key)
                    && values.property(value, account, true)?
                {
                    break;
                }
            }
        }
        "space_id" => {
            if !node
                .properties
                .get("space_id")
                .map(|value| values.property(value, account, true))
                .transpose()?
                .unwrap_or(false)
            {
                values.push("default")?;
            }
        }
        "labels" => {
            if let (Some(rel_type), Some(label)) =
                (catalog.rel_type_id("HAS_LABEL"), catalog.label_id("Label"))
            {
                let properties = BTreeMap::new();
                for direction in [
                    hawdb_core::RelationshipDirection::Outgoing,
                    hawdb_core::RelationshipDirection::Incoming,
                ] {
                    visit_one_hop_relationships_with_budget(
                        store,
                        OneHopRelationshipSpec {
                            source: node.id,
                            rel_type_id: Some(rel_type),
                            target_label_ids: Some(&[label]),
                            rel_properties: &properties,
                            relationship_scan_filter: None,
                            direction,
                        },
                        AdjacencyReadMemory {
                            budget_bytes,
                            account: Some(account),
                        },
                        &hawdb_executor::observer::NoopExecutionObserver,
                        &mut |_, label| {
                            for key in ["canonical_name", "name", "id"] {
                                if let Some(value) = label.properties.get(key) {
                                    let (text, _allocation) = property_text(value, account, None)?;
                                    if !text.is_empty() {
                                        if !values.values.iter().any(|value| value == &*text) {
                                            values.push(&text)?;
                                        }
                                        break;
                                    }
                                }
                            }
                            Ok(ScanControl::Continue)
                        },
                    )?;
                }
                values.values.sort_unstable();
            }
        }
        _ => {
            if let Some(value) = node.properties.get(key) {
                values.property(value, account, false)?;
            }
        }
    }
    Ok(values)
}
