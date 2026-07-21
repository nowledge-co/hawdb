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
}
