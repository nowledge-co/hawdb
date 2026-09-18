// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::search::{RuleEvent, RuleOutcome};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const IN_SUFFIX: &str = "__in";
const NOT_IN_SUFFIX: &str = "__not_in";
const GT_SUFFIX: &str = "__gt";
const GTE_SUFFIX: &str = "__gte";
const LT_SUFFIX: &str = "__lt";
const LTE_SUFFIX: &str = "__lte";
const EXISTS_SUFFIX: &str = "__exists";
const MISSING_SUFFIX: &str = "__missing";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchFieldRef {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SearchScalarValue {
    String(String),
    Enum(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPredicateOp {
    Eq(SearchScalarValue),
    In(BTreeSet<SearchScalarValue>),
    NotIn(BTreeSet<SearchScalarValue>),
    Gt(SearchScalarValue),
    Gte(SearchScalarValue),
    Lt(SearchScalarValue),
    Lte(SearchScalarValue),
    Exists,
    IsMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPredicate {
    field: SearchFieldRef,
    op: SearchPredicateOp,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchPredicateSet {
    predicates: Vec<SearchPredicate>,
    unsatisfiable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchScanPredicateSupport {
    pub equality: bool,
    pub in_list: bool,
    pub not_in_list: bool,
    pub range: bool,
    pub presence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPredicatePushdown {
    pushed: SearchPredicateSet,
    residual: SearchPredicateSet,
    events: Vec<RuleEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPredicateParseError {
    filter_key: String,
    reason: &'static str,
}

impl SearchFieldRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl SearchScalarValue {
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    pub fn enumeration(value: impl Into<String>) -> Self {
        Self::Enum(value.into())
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(value) | Self::Enum(value) => value,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::Enum(_) => "enum",
        }
    }
}

impl SearchPredicate {
    pub fn eq(field: impl Into<String>, value: impl Into<String>) -> Self {
        let field = field.into();
        Self {
            field: SearchFieldRef::new(field.clone()),
            op: SearchPredicateOp::Eq(search_scalar_value_for_field(&field, value.into())),
        }
    }

    pub fn in_list(field: impl Into<String>, values: impl IntoIterator<Item = String>) -> Self {
        let field = field.into();
        Self {
            field: SearchFieldRef::new(field.clone()),
            op: SearchPredicateOp::In(
                values
                    .into_iter()
                    .map(|value| search_scalar_value_for_field(&field, value))
                    .collect(),
            ),
        }
    }

    pub fn not_in_list(field: impl Into<String>, values: impl IntoIterator<Item = String>) -> Self {
        let field = field.into();
        Self {
            field: SearchFieldRef::new(field.clone()),
            op: SearchPredicateOp::NotIn(
                values
                    .into_iter()
                    .map(|value| search_scalar_value_for_field(&field, value))
                    .collect(),
            ),
        }
    }

    pub fn gt(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: SearchFieldRef::new(field),
            op: SearchPredicateOp::Gt(SearchScalarValue::string(value)),
        }
    }

    pub fn gte(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: SearchFieldRef::new(field),
            op: SearchPredicateOp::Gte(SearchScalarValue::string(value)),
        }
    }

    pub fn lt(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: SearchFieldRef::new(field),
            op: SearchPredicateOp::Lt(SearchScalarValue::string(value)),
        }
    }

    pub fn lte(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: SearchFieldRef::new(field),
            op: SearchPredicateOp::Lte(SearchScalarValue::string(value)),
        }
    }

    pub fn exists(field: impl Into<String>) -> Self {
        Self {
            field: SearchFieldRef::new(field),
            op: SearchPredicateOp::Exists,
        }
    }

    pub fn is_missing(field: impl Into<String>) -> Self {
        Self {
            field: SearchFieldRef::new(field),
            op: SearchPredicateOp::IsMissing,
        }
    }

    pub fn field(&self) -> &SearchFieldRef {
        &self.field
    }

    pub fn op(&self) -> &SearchPredicateOp {
        &self.op
    }

    fn pushdown_supported(&self, support: SearchScanPredicateSupport) -> bool {
        match self.op {
            SearchPredicateOp::Eq(_) => support.equality,
            SearchPredicateOp::In(_) => support.in_list,
            SearchPredicateOp::NotIn(_) => support.not_in_list,
            SearchPredicateOp::Gt(_)
            | SearchPredicateOp::Gte(_)
            | SearchPredicateOp::Lt(_)
            | SearchPredicateOp::Lte(_) => support.range,
            SearchPredicateOp::Exists | SearchPredicateOp::IsMissing => support.presence,
        }
    }
}

fn search_scalar_value_for_field(field: &str, value: String) -> SearchScalarValue {
    if search_field_is_enum_like(field) {
        SearchScalarValue::enumeration(normalize_search_enum_value(&value))
    } else {
        SearchScalarValue::string(value)
    }
}

pub fn search_field_is_enum_like(field: &str) -> bool {
    matches!(
        field,
        "kind" | "labels" | "unit_type" | "lifecycle_state" | "review_status" | "temporal_context"
    )
}

pub fn normalize_search_enum_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

impl SearchPredicateSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn unsatisfiable() -> Self {
        Self {
            predicates: Vec::new(),
            unsatisfiable: true,
        }
    }

    pub fn new(predicates: Vec<SearchPredicate>) -> Self {
        Self {
            predicates,
            unsatisfiable: false,
        }
    }

    pub fn from_metadata_filters(
        filters: &BTreeMap<String, String>,
    ) -> Result<Self, SearchPredicateParseError> {
        let mut predicates = Vec::new();
        for (key, value) in filters {
            if let Some(predicate) = mem_search_filter_alias_predicate(key, value)? {
                predicates.push(predicate);
                continue;
            }
            if let Some(field) = key.strip_suffix(IN_SUFFIX) {
                let values = parse_string_list_filter(key, value)?;
                let field = canonical_search_filter_field(field);
                let values = normalize_search_filter_values(key, field, values)?;
                if values.is_empty() {
                    return Ok(Self::unsatisfiable());
                }
                predicates.push(SearchPredicate::in_list(field, values));
            } else if let Some(field) = key.strip_suffix(NOT_IN_SUFFIX) {
                let values = parse_string_list_filter(key, value)?;
                let field = canonical_search_filter_field(field);
                let values = normalize_search_filter_values(key, field, values)?;
                if !values.is_empty() {
                    predicates.push(SearchPredicate::not_in_list(field, values));
                }
            } else if let Some(field) = key.strip_suffix(EXISTS_SUFFIX) {
                let exists = parse_bool_filter_value(key, value)?;
                let field = canonical_search_filter_field(field);
                predicates.push(if exists {
                    SearchPredicate::exists(field)
                } else {
                    SearchPredicate::is_missing(field)
                });
            } else if let Some(field) = key.strip_suffix(MISSING_SUFFIX) {
                let missing = parse_bool_filter_value(key, value)?;
                let field = canonical_search_filter_field(field);
                predicates.push(if missing {
                    SearchPredicate::is_missing(field)
                } else {
                    SearchPredicate::exists(field)
                });
            } else if let Some(field) = key.strip_suffix(GTE_SUFFIX) {
                reject_boolean_alias_range_filter(key, field)?;
                predicates.push(SearchPredicate::gte(field, value));
            } else if let Some(field) = key.strip_suffix(GT_SUFFIX) {
                reject_boolean_alias_range_filter(key, field)?;
                predicates.push(SearchPredicate::gt(field, value));
            } else if let Some(field) = key.strip_suffix(LTE_SUFFIX) {
                reject_boolean_alias_range_filter(key, field)?;
                predicates.push(SearchPredicate::lte(field, value));
            } else if let Some(field) = key.strip_suffix(LT_SUFFIX) {
                reject_boolean_alias_range_filter(key, field)?;
                predicates.push(SearchPredicate::lt(field, value));
            } else {
                let field = canonical_search_filter_field(key);
                let value = normalize_search_filter_value(key, field, value.clone())?;
                predicates.push(SearchPredicate::eq(field, value));
            }
        }
        Ok(Self::new(predicates))
    }

    pub fn predicates(&self) -> &[SearchPredicate] {
        &self.predicates
    }

    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty() && !self.unsatisfiable
    }

    pub fn is_unsatisfiable(&self) -> bool {
        self.unsatisfiable
    }
}

impl Default for SearchScanPredicateSupport {
    fn default() -> Self {
        Self {
            equality: true,
            in_list: true,
            not_in_list: true,
            range: true,
            presence: true,
        }
    }
}

impl SearchPredicatePushdown {
    pub fn pushed(&self) -> &SearchPredicateSet {
        &self.pushed
    }

    pub fn residual(&self) -> &SearchPredicateSet {
        &self.residual
    }

    pub fn events(&self) -> &[RuleEvent] {
        &self.events
    }
}

impl SearchPredicateParseError {
    pub fn filter_key(&self) -> &str {
        &self.filter_key
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl fmt::Display for SearchPredicateParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.filter_key, self.reason)
    }
}

impl std::error::Error for SearchPredicateParseError {}

pub fn push_search_predicates(
    predicates: &SearchPredicateSet,
    support: SearchScanPredicateSupport,
) -> SearchPredicatePushdown {
    if predicates.is_unsatisfiable() {
        return SearchPredicatePushdown {
            pushed: SearchPredicateSet::unsatisfiable(),
            residual: SearchPredicateSet::empty(),
            events: vec![RuleEvent::new(
                "transformation:push_search_predicates",
                RuleOutcome::Applied,
                "input predicate set is unsatisfiable",
            )],
        };
    }

    let mut pushed = Vec::new();
    let mut residual = Vec::new();
    for predicate in predicates.predicates() {
        if predicate.pushdown_supported(support) {
            pushed.push(predicate.clone());
        } else {
            residual.push(predicate.clone());
        }
    }

    let pushed_count = pushed.len();
    let residual_count = residual.len();
    let outcome = if pushed_count == 0 {
        RuleOutcome::Skipped
    } else {
        RuleOutcome::Applied
    };
    SearchPredicatePushdown {
        pushed: SearchPredicateSet::new(pushed),
        residual: SearchPredicateSet::new(residual),
        events: vec![RuleEvent::new(
            "transformation:push_search_predicates",
            outcome,
            format!("pushed={pushed_count} residual={residual_count}"),
        )],
    }
}

fn parse_string_list_filter(
    filter_key: &str,
    value: &str,
) -> Result<Vec<String>, SearchPredicateParseError> {
    serde_json::from_str::<Vec<String>>(value).map_err(|_| SearchPredicateParseError {
        filter_key: filter_key.to_string(),
        reason: "expected JSON string array",
    })
}

fn mem_search_filter_alias_predicate(
    key: &str,
    value: &str,
) -> Result<Option<SearchPredicate>, SearchPredicateParseError> {
    let predicate = match key {
        "event_date_from" | "event_date__gte" => SearchPredicate::gte("event_end", value),
        "event_date_after" | "event_date__gt" => SearchPredicate::gt("event_end", value),
        "event_date_to" | "event_date__lte" => SearchPredicate::lte("event_start", value),
        "event_date_before" | "event_date__lt" => SearchPredicate::lt("event_start", value),
        "recorded_date_from" | "recorded_date__gte" => SearchPredicate::gte("created_at", value),
        "recorded_date_after" | "recorded_date__gt" => SearchPredicate::gt("created_at", value),
        "recorded_date_to" | "recorded_date__lte" => SearchPredicate::lte("created_at", value),
        "recorded_date_before" | "recorded_date__lt" => SearchPredicate::lt("created_at", value),
        "event_date__in" | "event_date__not_in" | "recorded_date__in" | "recorded_date__not_in" => {
            return Err(SearchPredicateParseError {
                filter_key: key.to_string(),
                reason: "date alias filter only supports range predicates",
            });
        }
        _ => return Ok(None),
    };
    Ok(Some(predicate))
}

fn canonical_search_filter_field(field: &str) -> &str {
    match field {
        "latest" | "history" => "is_latest",
        "space" | "spaces" | "space_ids" => "space_id",
        "temporal" | "temporal_contexts" => "temporal_context",
        _ => field,
    }
}

fn reject_boolean_alias_range_filter(
    filter_key: &str,
    field: &str,
) -> Result<(), SearchPredicateParseError> {
    if matches!(field, "history" | "latest" | "is_latest") {
        return Err(SearchPredicateParseError {
            filter_key: filter_key.to_string(),
            reason: "boolean filter only supports equality and list predicates",
        });
    }
    Ok(())
}

fn normalize_search_filter_values(
    filter_key: &str,
    field: &str,
    values: Vec<String>,
) -> Result<Vec<String>, SearchPredicateParseError> {
    values
        .into_iter()
        .map(|value| normalize_search_filter_value(filter_key, field, value))
        .collect()
}

fn normalize_search_filter_value(
    filter_key: &str,
    field: &str,
    value: String,
) -> Result<String, SearchPredicateParseError> {
    if filter_key == "history"
        || filter_key
            .strip_suffix(IN_SUFFIX)
            .is_some_and(|field| field == "history")
        || filter_key
            .strip_suffix(NOT_IN_SUFFIX)
            .is_some_and(|field| field == "history")
    {
        return invert_history_filter_value(filter_key, &value);
    }
    if field == "is_latest" {
        return normalize_bool_filter_value(filter_key, &value);
    }
    Ok(value)
}

fn invert_history_filter_value(
    filter_key: &str,
    value: &str,
) -> Result<String, SearchPredicateParseError> {
    let value = parse_bool_filter_value(filter_key, value)?;
    Ok((!value).to_string())
}

fn normalize_bool_filter_value(
    filter_key: &str,
    value: &str,
) -> Result<String, SearchPredicateParseError> {
    parse_bool_filter_value(filter_key, value).map(|value| value.to_string())
}

fn parse_bool_filter_value(
    filter_key: &str,
    value: &str,
) -> Result<bool, SearchPredicateParseError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(SearchPredicateParseError {
            filter_key: filter_key.to_string(),
            reason: "expected boolean filter value",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        push_search_predicates, SearchPredicateOp, SearchPredicateSet, SearchScanPredicateSupport,
    };
    use std::collections::BTreeMap;

    #[test]
    fn metadata_filters_lower_to_typed_search_predicates() {
        let filters = BTreeMap::from([
            ("kind".to_string(), "Memory".to_string()),
            (
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            ),
            (
                "unit_type__in".to_string(),
                r#"["fact","learning"]"#.to_string(),
            ),
            ("created_at__gte".to_string(), "1710000000".to_string()),
            ("updated_at__lt".to_string(), "1720000000".to_string()),
        ]);

        let predicates = SearchPredicateSet::from_metadata_filters(&filters).unwrap();

        assert_eq!(predicates.predicates().len(), 5);
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::Gte(_)
        ));
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::Eq(_)
        ));
        assert!(matches!(
            predicates.predicates()[2].op(),
            SearchPredicateOp::NotIn(values)
                if values.len() == 2 && values.iter().all(|value| value.kind() == "enum")
        ));
        assert!(matches!(
            predicates.predicates()[3].op(),
            SearchPredicateOp::In(values)
                if values.len() == 2 && values.iter().all(|value| value.kind() == "enum")
        ));
        assert!(matches!(
            predicates.predicates()[4].op(),
            SearchPredicateOp::Lt(_)
        ));
    }

    #[test]
    fn metadata_filters_canonicalize_latest_and_history_aliases() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("history".to_string(), "true".to_string()),
            ("latest__in".to_string(), r#"["true","0"]"#.to_string()),
        ]))
        .unwrap();

        assert_eq!(predicates.predicates().len(), 2);
        assert_eq!(predicates.predicates()[0].field().name(), "is_latest");
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::Eq(value) if value.as_str() == "false"
        ));
        assert_eq!(predicates.predicates()[1].field().name(), "is_latest");
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::In(values)
                if values.iter().map(|value| value.as_str()).collect::<Vec<_>>()
                    == vec!["false", "true"]
        ));
    }

    #[test]
    fn metadata_filters_reject_boolean_alias_ranges() {
        let error = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([(
            "history__gte".to_string(),
            "true".to_string(),
        )]))
        .unwrap_err();

        assert_eq!(error.filter_key(), "history__gte");
        assert_eq!(
            error.reason(),
            "boolean filter only supports equality and list predicates"
        );
    }

    #[test]
    fn metadata_filters_lower_mem_date_aliases_to_timestamp_ranges() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            (
                "event_date_from".to_string(),
                "2026-07-01T00:00:00Z".to_string(),
            ),
            (
                "event_date_to".to_string(),
                "2026-07-02T00:00:00Z".to_string(),
            ),
            ("recorded_date_from".to_string(), "2026-06-01".to_string()),
            ("recorded_date_to".to_string(), "2026-06-30".to_string()),
        ]))
        .unwrap();

        assert_eq!(predicates.predicates().len(), 4);
        assert_eq!(predicates.predicates()[0].field().name(), "event_end");
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::Gte(_)
        ));
        assert_eq!(predicates.predicates()[1].field().name(), "event_start");
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::Lte(_)
        ));
        assert_eq!(predicates.predicates()[2].field().name(), "created_at");
        assert!(matches!(
            predicates.predicates()[2].op(),
            SearchPredicateOp::Gte(_)
        ));
        assert_eq!(predicates.predicates()[3].field().name(), "created_at");
        assert!(matches!(
            predicates.predicates()[3].op(),
            SearchPredicateOp::Lte(_)
        ));
    }

    #[test]
    fn metadata_filters_reject_date_alias_lists() {
        let error = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([(
            "event_date__in".to_string(),
            r#"["2026-07-01"]"#.to_string(),
        )]))
        .unwrap_err();

        assert_eq!(error.filter_key(), "event_date__in");
        assert_eq!(
            error.reason(),
            "date alias filter only supports range predicates"
        );
    }

    #[test]
    fn metadata_filters_canonicalize_temporal_context_aliases() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("temporal".to_string(), " Recent ".to_string()),
            (
                "temporal_contexts__in".to_string(),
                r#"["Current","Archived"]"#.to_string(),
            ),
        ]))
        .unwrap();

        assert_eq!(predicates.predicates().len(), 2);
        assert_eq!(
            predicates.predicates()[0].field().name(),
            "temporal_context"
        );
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::Eq(value) if value.kind() == "enum" && value.as_str() == "recent"
        ));
        assert_eq!(
            predicates.predicates()[1].field().name(),
            "temporal_context"
        );
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::In(values)
                if values.iter().all(|value| value.kind() == "enum")
                    && values.iter().map(|value| value.as_str()).collect::<Vec<_>>()
                        == vec!["archived", "current"]
        ));
    }

    #[test]
    fn metadata_filters_treat_labels_as_enum_predicates() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("labels".to_string(), " Database ".to_string()),
            ("labels__in".to_string(), r#"["Rust","Graph"]"#.to_string()),
            (
                "labels__not_in".to_string(),
                r#"["Deleted","Forgotten"]"#.to_string(),
            ),
        ]))
        .unwrap();

        assert_eq!(predicates.predicates().len(), 3);
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::Eq(value)
                if value.kind() == "enum" && value.as_str() == "database"
        ));
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::In(values)
                if values.iter().all(|value| value.kind() == "enum")
                    && values.iter().map(|value| value.as_str()).collect::<Vec<_>>()
                        == vec!["graph", "rust"]
        ));
        assert!(matches!(
            predicates.predicates()[2].op(),
            SearchPredicateOp::NotIn(values)
                if values.iter().all(|value| value.kind() == "enum")
                    && values.iter().map(|value| value.as_str()).collect::<Vec<_>>()
                        == vec!["deleted", "forgotten"]
        ));
    }

    #[test]
    fn metadata_filters_canonicalize_space_scope_aliases() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("space".to_string(), "Default".to_string()),
            (
                "space_ids__in".to_string(),
                r#"["Default","Team"]"#.to_string(),
            ),
        ]))
        .unwrap();

        assert_eq!(predicates.predicates().len(), 2);
        assert_eq!(predicates.predicates()[0].field().name(), "space_id");
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::Eq(value) if value.kind() == "string" && value.as_str() == "Default"
        ));
        assert_eq!(predicates.predicates()[1].field().name(), "space_id");
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::In(values)
                if values.iter().all(|value| value.kind() == "string")
                    && values.iter().map(|value| value.as_str()).collect::<Vec<_>>()
                        == vec!["Default", "Team"]
        ));
    }

    #[test]
    fn metadata_filters_lower_exists_and_missing_predicates() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("source_id__exists".to_string(), "true".to_string()),
            ("labels__missing".to_string(), "1".to_string()),
            ("confidence__exists".to_string(), "false".to_string()),
            ("importance__missing".to_string(), "0".to_string()),
        ]))
        .unwrap();

        assert_eq!(predicates.predicates().len(), 4);
        assert_eq!(predicates.predicates()[0].field().name(), "confidence");
        assert!(matches!(
            predicates.predicates()[0].op(),
            SearchPredicateOp::IsMissing
        ));
        assert_eq!(predicates.predicates()[1].field().name(), "importance");
        assert!(matches!(
            predicates.predicates()[1].op(),
            SearchPredicateOp::Exists
        ));
        assert_eq!(predicates.predicates()[2].field().name(), "labels");
        assert!(matches!(
            predicates.predicates()[2].op(),
            SearchPredicateOp::IsMissing
        ));
        assert_eq!(predicates.predicates()[3].field().name(), "source_id");
        assert!(matches!(
            predicates.predicates()[3].op(),
            SearchPredicateOp::Exists
        ));
    }

    #[test]
    fn metadata_filters_reject_malformed_presence_predicates() {
        let error = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([(
            "source_id__exists".to_string(),
            "maybe".to_string(),
        )]))
        .unwrap_err();

        assert_eq!(error.filter_key(), "source_id__exists");
        assert_eq!(error.reason(), "expected boolean filter value");
    }

    #[test]
    fn malformed_list_filters_are_parse_errors() {
        let filters = BTreeMap::from([(
            "lifecycle_state__not_in".to_string(),
            "deleted,forgotten".to_string(),
        )]);

        let error = SearchPredicateSet::from_metadata_filters(&filters).unwrap_err();

        assert_eq!(error.filter_key(), "lifecycle_state__not_in");
        assert_eq!(error.reason(), "expected JSON string array");
    }

    #[test]
    fn predicate_pushdown_splits_unsupported_predicates() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            (
                "lifecycle_state__not_in".to_string(),
                r#"["deleted"]"#.to_string(),
            ),
        ]))
        .unwrap();

        let pushdown = push_search_predicates(
            &predicates,
            SearchScanPredicateSupport {
                equality: true,
                in_list: true,
                not_in_list: false,
                range: true,
                presence: true,
            },
        );

        assert_eq!(pushdown.pushed().predicates().len(), 1);
        assert_eq!(pushdown.residual().predicates().len(), 1);
        assert_eq!(pushdown.events()[0].detail(), "pushed=1 residual=1");
    }

    #[test]
    fn empty_in_list_is_unsatisfiable() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([(
            "unit_type__in".to_string(),
            "[]".to_string(),
        )]))
        .unwrap();

        assert!(predicates.is_unsatisfiable());
    }

    #[test]
    fn predicate_pushdown_splits_unsupported_range_predicates() {
        let predicates = SearchPredicateSet::from_metadata_filters(&BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("created_at__gte".to_string(), "1710000000".to_string()),
        ]))
        .unwrap();

        let pushdown = push_search_predicates(
            &predicates,
            SearchScanPredicateSupport {
                equality: true,
                in_list: true,
                not_in_list: true,
                range: false,
                presence: true,
            },
        );

        assert_eq!(pushdown.pushed().predicates().len(), 1);
        assert_eq!(pushdown.residual().predicates().len(), 1);
        assert!(matches!(
            pushdown.residual().predicates()[0].op(),
            SearchPredicateOp::Gte(_)
        ));
    }
}
