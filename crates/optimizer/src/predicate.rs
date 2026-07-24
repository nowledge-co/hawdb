use crate::search::{RuleEvent, RuleOutcome};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const IN_SUFFIX: &str = "__in";
const NOT_IN_SUFFIX: &str = "__not_in";
const GT_SUFFIX: &str = "__gt";
const GTE_SUFFIX: &str = "__gte";
const LT_SUFFIX: &str = "__lt";
const LTE_SUFFIX: &str = "__lte";

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
        "kind" | "unit_type" | "lifecycle_state" | "review_status" | "temporal_context"
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
            if let Some(field) = key.strip_suffix(IN_SUFFIX) {
                let values = parse_string_list_filter(key, value)?;
                if values.is_empty() {
                    return Ok(Self::unsatisfiable());
                }
                predicates.push(SearchPredicate::in_list(field, values));
            } else if let Some(field) = key.strip_suffix(NOT_IN_SUFFIX) {
                let values = parse_string_list_filter(key, value)?;
                if !values.is_empty() {
                    predicates.push(SearchPredicate::not_in_list(field, values));
                }
            } else if let Some(field) = key.strip_suffix(GTE_SUFFIX) {
                predicates.push(SearchPredicate::gte(field, value));
            } else if let Some(field) = key.strip_suffix(GT_SUFFIX) {
                predicates.push(SearchPredicate::gt(field, value));
            } else if let Some(field) = key.strip_suffix(LTE_SUFFIX) {
                predicates.push(SearchPredicate::lte(field, value));
            } else if let Some(field) = key.strip_suffix(LT_SUFFIX) {
                predicates.push(SearchPredicate::lt(field, value));
            } else {
                predicates.push(SearchPredicate::eq(key, value));
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
