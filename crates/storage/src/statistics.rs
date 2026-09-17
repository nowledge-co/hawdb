//! Graph index statistics sampling and full-text tokenization.

use crate::{CowSegment, CowSegmentedMap, NodeId, NodeRecord};
use skein_core::{Catalog, IndexId, IndexKind, IndexStatisticsSample, LabelId, Value};
use std::collections::{BTreeMap, BTreeSet};

type NodePropertyIndex = CowSegmentedMap<(LabelId, String, Value), CowSegment<BTreeSet<NodeId>>>;
type CompositePropertyIndex =
    CowSegmentedMap<(LabelId, Vec<(String, Value)>), CowSegment<BTreeSet<NodeId>>>;

pub fn composite_property_index_key(
    node: &NodeRecord,
    properties: &[String],
) -> Option<Vec<(String, Value)>> {
    properties
        .iter()
        .map(|property| {
            node.properties
                .get(property)
                .cloned()
                .map(|value| (property.clone(), value))
        })
        .collect()
}

pub fn scalar_property_index_cardinality(
    index: &NodePropertyIndex,
    label_id: LabelId,
    property: &str,
) -> (u64, u64) {
    index
        .iter()
        .filter(|((candidate_label, candidate_property, _), _)| {
            *candidate_label == label_id && candidate_property == property
        })
        .fold((0_u64, 0_u64), |(size, unique), (_, node_ids)| {
            (
                size.saturating_add(node_ids.len() as u64),
                unique.saturating_add(1),
            )
        })
}

pub fn composite_property_index_unique_values(
    index: &CompositePropertyIndex,
    label_id: LabelId,
    properties: &[String],
) -> u64 {
    index
        .keys()
        .filter(|(candidate_label, key)| {
            *candidate_label == label_id
                && key
                    .iter()
                    .map(|(property, _)| property)
                    .eq(properties.iter())
        })
        .count() as u64
}

pub fn compute_index_statistics_samples(
    catalog: &Catalog,
    property_index: &NodePropertyIndex,
    composite_property_index: &CompositePropertyIndex,
) -> BTreeMap<IndexId, IndexStatisticsSample> {
    let mut samples = BTreeMap::new();
    for index in catalog
        .property_indexes()
        .filter(|index| index.kind != IndexKind::FullText)
    {
        let (index_size, unique_values) =
            scalar_property_index_cardinality(property_index, index.label_id, &index.property);
        samples.insert(
            index.id,
            IndexStatisticsSample::exact(index_size, unique_values),
        );
    }
    for index in catalog.composite_property_indexes() {
        let index_size = composite_property_index
            .iter()
            .filter(|((candidate_label, key), _)| {
                *candidate_label == index.label_id
                    && key
                        .iter()
                        .map(|(property, _)| property)
                        .eq(index.properties.iter())
            })
            .fold(0_u64, |size, (_, node_ids)| {
                size.saturating_add(node_ids.len() as u64)
            });
        let unique_values = composite_property_index_unique_values(
            composite_property_index,
            index.label_id,
            &index.properties,
        );
        samples.insert(
            index.id,
            IndexStatisticsSample::exact(index_size, unique_values),
        );
    }
    samples
}

pub fn full_text_index_tokens(value: &str) -> BTreeSet<String> {
    let normalized = value.to_lowercase();
    let chars = normalized.chars().collect::<Vec<_>>();
    let mut tokens = BTreeSet::new();
    for start in 0..chars.len() {
        for width in 1..=3 {
            let end = start + width;
            if end > chars.len() {
                break;
            }
            let token = chars[start..end].iter().collect::<String>();
            if !token.chars().all(char::is_whitespace) {
                tokens.insert(token);
            }
        }
    }
    tokens
}

pub fn full_text_query_tokens(query: &str) -> Vec<String> {
    full_text_index_tokens(query).into_iter().collect()
}
