//! Projection, aggregation, predicate, and filter evaluation helpers.

use crate::binding::Binding;
use crate::observer::ExecutionObserver;
use crate::predicate::{
    combine_property_filters, compare_property_values, label_ids_for_pattern,
    property_filter_from_properties,
};
use crate::scan::is_null_lookup_node;
use crate::store::GraphExecutionRead;
use crate::traversal::one_hop_relationships;
use skein_core::{Catalog, RelationshipDirection, Result, SkeinError, Value};
use skein_plan::{
    AggregateFunction, AggregateTarget, Aggregation, CoalesceDifferenceProjectionTerm,
    ComparisonOp, DatePart, Predicate, Projection, ProjectionExpression, SortDirection, SortItem,
    SortKey,
};
use skein_storage::{NodeRecord, PropertyFilter, RelRecord};
use std::collections::BTreeMap;

type ValueRangeBound = (Value, bool);
type ValueRangeBounds = (Option<ValueRangeBound>, Option<ValueRangeBound>);

mod predicate;
mod value;

pub use predicate::*;
pub use value::*;
