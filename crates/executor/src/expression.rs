//! Projection, aggregation, predicate, and filter evaluation helpers.

use crate::binding::Binding;
use crate::memory::DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES;
use crate::observer::ExecutionObserver;
use crate::predicate::{
    combine_property_filters, compare_property_values, label_ids_for_pattern,
    property_filter_from_properties,
};
use crate::store::{AdjacencyReadMemory, GraphExecutionRead, ScanControl};
use crate::traversal::{visit_one_hop_relationships_with_budget, OneHopRelationshipSpec};
use skein_core::{Catalog, RelationshipDirection, Result, SkeinError, Value, ValueRef};
use skein_plan::{
    CoalesceDifferenceProjectionTerm, ComparisonOp, DatePart, Predicate, Projection,
    ProjectionExpression, SortDirection, SortItem, SortKey,
};
use skein_storage::{NodeRecord, PropertyFilter, RelRecord};
use std::collections::BTreeMap;

type ValueRangeBound = (Value, bool);
type ValueRangeBounds = (Option<ValueRangeBound>, Option<ValueRangeBound>);

mod predicate;
mod value;

pub use predicate::*;
pub use value::*;
