#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchFilterTarget {
    Field(String),
    JsonMetadataPath(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchFilterOp {
    Eq(String),
    Gte(String),
    In(Vec<String>),
    NotIn(Vec<String>),
    InvalidIn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchFilterPredicate {
    pub(crate) target: SearchFilterTarget,
    pub(crate) op: SearchFilterOp,
}

impl SearchFilterPredicate {
    pub(crate) fn parse(key: &str, value: &str) -> Self {
        let (field, op) = if let Some(field) = key.strip_suffix("__gte") {
            (field, SearchFilterOp::Gte(value.to_string()))
        } else if let Some(field) = key.strip_suffix("__not_in") {
            let op = serde_json::from_str::<Vec<String>>(value)
                .map(SearchFilterOp::NotIn)
                .unwrap_or(SearchFilterOp::InvalidIn);
            (field, op)
        } else if let Some(field) = key.strip_suffix("__in") {
            let op = serde_json::from_str::<Vec<String>>(value)
                .map(SearchFilterOp::In)
                .unwrap_or(SearchFilterOp::InvalidIn);
            (field, op)
        } else {
            (key, SearchFilterOp::Eq(value.to_string()))
        };

        let target = field
            .strip_prefix("metadata.")
            .map(|path| SearchFilterTarget::JsonMetadataPath(path.to_string()))
            .unwrap_or_else(|| SearchFilterTarget::Field(field.to_string()));

        Self { target, op }
    }

    pub(crate) fn exact_values(&self) -> Option<&[String]> {
        match &self.op {
            SearchFilterOp::Eq(value) => Some(std::slice::from_ref(value)),
            SearchFilterOp::In(values) => Some(values.as_slice()),
            SearchFilterOp::Gte(_) | SearchFilterOp::NotIn(_) | SearchFilterOp::InvalidIn => None,
        }
    }

    pub(crate) fn gte_value(&self) -> Option<&str> {
        match &self.op {
            SearchFilterOp::Gte(value) => Some(value),
            SearchFilterOp::Eq(_)
            | SearchFilterOp::In(_)
            | SearchFilterOp::NotIn(_)
            | SearchFilterOp::InvalidIn => None,
        }
    }

    pub(crate) fn excluded_values(&self) -> Option<&[String]> {
        match &self.op {
            SearchFilterOp::NotIn(values) => Some(values.as_slice()),
            SearchFilterOp::Eq(_)
            | SearchFilterOp::Gte(_)
            | SearchFilterOp::In(_)
            | SearchFilterOp::InvalidIn => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_field_equality_predicate() {
        assert_eq!(
            SearchFilterPredicate::parse("unit_type", "decision"),
            SearchFilterPredicate {
                target: SearchFilterTarget::Field("unit_type".to_string()),
                op: SearchFilterOp::Eq("decision".to_string()),
            }
        );
    }

    #[test]
    fn parses_field_range_predicate() {
        assert_eq!(
            SearchFilterPredicate::parse("importance__gte", "0.7"),
            SearchFilterPredicate {
                target: SearchFilterTarget::Field("importance".to_string()),
                op: SearchFilterOp::Gte("0.7".to_string()),
            }
        );
    }

    #[test]
    fn parses_json_metadata_in_predicate() {
        assert_eq!(
            SearchFilterPredicate::parse("metadata.topic__in", r#"["bridge","search"]"#),
            SearchFilterPredicate {
                target: SearchFilterTarget::JsonMetadataPath("topic".to_string()),
                op: SearchFilterOp::In(vec!["bridge".to_string(), "search".to_string()]),
            }
        );
    }

    #[test]
    fn rejects_malformed_in_predicate() {
        assert_eq!(
            SearchFilterPredicate::parse("unit_type__in", "decision").op,
            SearchFilterOp::InvalidIn
        );
    }

    #[test]
    fn parses_field_not_in_predicate() {
        assert_eq!(
            SearchFilterPredicate::parse("lifecycle_state__not_in", r#"["deleted","forgotten"]"#),
            SearchFilterPredicate {
                target: SearchFilterTarget::Field("lifecycle_state".to_string()),
                op: SearchFilterOp::NotIn(vec!["deleted".to_string(), "forgotten".to_string()]),
            }
        );
    }
}
