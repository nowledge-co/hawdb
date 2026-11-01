// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Small owned proof sets. Statistics never establish semantic constraints.

use crate::graph::OptimizerCatalog;
use hawdb_core::{PropertyType, SchemaObjectState, TableKind, Value};
use hawdb_plan_cypher::{Predicate, Projection, ProjectionExpression, SortDirection, SortKey};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const MAX_FIELDS: usize = 64;
const MAX_KEYS: usize = 8;
const MAX_KEY_FIELDS: usize = 8;
const MAX_NAME_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Atom {
    Column(String),
    Property(String, String),
    Id(String),
}

fn name(value: &str) -> Option<String> {
    (value.len() <= MAX_NAME_BYTES).then(|| value.to_owned())
}
impl Atom {
    pub(super) fn expression(expression: &ProjectionExpression) -> Option<Self> {
        match expression {
            ProjectionExpression::Column(column) => Some(Self::Column(name(column)?)),
            ProjectionExpression::Property { variable, property } => {
                Some(Self::Property(name(variable)?, name(property)?))
            }
            ProjectionExpression::Id { variable } => Some(Self::Id(name(variable)?)),
            _ => None,
        }
    }
    pub(super) fn sort_key(key: &SortKey) -> Option<Self> {
        match key {
            SortKey::Column(column) => Some(Self::Column(name(column)?)),
            SortKey::Property { variable, property } => {
                Some(Self::Property(name(variable)?, name(property)?))
            }
            SortKey::Id { variable } => Some(Self::Id(name(variable)?)),
            SortKey::Expression(expression) => Self::expression(expression),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum Domain {
    Bool,
    Int,
    String,
    #[default]
    Unknown,
}
impl Domain {
    fn exact_literal(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (Self::Bool, Value::Bool(_))
                | (Self::Int, Value::Int(_))
                | (Self::String, Value::String(_))
        )
    }
}

#[derive(Debug, Default, Clone)]
pub(super) struct ScalarFact {
    pub(super) nonnull: bool,
    pub(super) domain: Domain,
    pub(super) constant: Option<Value>,
}
impl ScalarFact {
    fn literal(value: &Value) -> Self {
        let constant = match value {
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => Some(value.clone()),
            Value::String(value) if value.len() <= MAX_NAME_BYTES => {
                Some(Value::String(value.clone()))
            }
            _ => None,
        };
        Self {
            nonnull: !matches!(value, Value::Null),
            domain: match value {
                Value::Bool(_) => Domain::Bool,
                Value::Int(_) => Domain::Int,
                Value::String(_) => Domain::String,
                _ => Domain::Unknown,
            },
            constant,
        }
    }
}

#[derive(Debug, Clone)]
struct Binding {
    label: String,
    // A source constraint is not a stream key after a join has duplicated it.
    unique_rows: bool,
}

#[derive(Debug, Default)]
pub(super) struct Facts {
    values: BTreeMap<Atom, ScalarFact>,
    bindings: BTreeMap<String, Binding>,
    keys: Vec<Vec<Atom>>,
    equivalents: BTreeMap<Atom, Atom>,
    pub(super) ordering: Vec<(Atom, SortDirection)>,
    pub(super) safe: bool,
    pub(super) values_only: bool,
    pub(super) max_rows: Option<usize>,
}

impl Facts {
    pub(super) fn scan(variable: &str, label: &str) -> Self {
        let (Some(variable), Some(label)) = (name(variable), name(label)) else {
            return Self::default();
        };
        Self {
            bindings: BTreeMap::from([(
                variable.clone(),
                Binding {
                    label,
                    unique_rows: true,
                },
            )]),
            keys: vec![vec![Atom::Id(variable)]],
            safe: true,
            ..Self::default()
        }
    }

    fn canonical<'a>(&'a self, atom: &'a Atom) -> &'a Atom {
        self.equivalents.get(atom).unwrap_or(atom)
    }
    pub(super) fn equivalent(&self, a: &Atom, b: &Atom) -> bool {
        self.canonical(a) == self.canonical(b)
    }

    pub(super) fn scalar(&self, atom: &Atom, catalog: &OptimizerCatalog) -> Option<ScalarFact> {
        if let Some(fact) = self.values.get(self.canonical(atom)) {
            return Some(fact.clone());
        }
        match atom {
            Atom::Column(_) => None,
            Atom::Id(variable) => self.bindings.get(variable).map(|_| ScalarFact {
                nonnull: true,
                domain: Domain::Int,
                constant: None,
            }),
            Atom::Property(variable, property) => {
                let binding = self.bindings.get(variable)?;
                let mut result = ScalarFact::default();
                if let Some(schema) = &catalog.source_schema {
                    let table = schema.table_id(TableKind::Node, &binding.label);
                    if let Some(table) = table.filter(|table| {
                        schema
                            .table_descriptor(*table)
                            .is_some_and(|table| table.state == SchemaObjectState::Public)
                    }) && let Some(descriptor) = schema
                        .property_descriptor_id(table, property)
                        .and_then(|id| schema.property_descriptor(id))
                        .filter(|property| property.state == SchemaObjectState::Public)
                    {
                        result.nonnull = !descriptor.nullable;
                        result.domain = match descriptor.value_type {
                            PropertyType::Bool => Domain::Bool,
                            PropertyType::Int => Domain::Int,
                            PropertyType::String => Domain::String,
                            _ => Domain::Unknown,
                        };
                        if let Some(label) = schema.label_id(&binding.label) {
                            result.nonnull |= schema
                                .node_property_exists_constraint_id(label, property)
                                .is_some();
                        }
                    }
                }
                Some(result)
            }
        }
    }

    pub(super) fn expression(
        &self,
        expression: &ProjectionExpression,
        catalog: &OptimizerCatalog,
    ) -> Option<ScalarFact> {
        self.expression_inner(expression, catalog, 0)
    }
    fn expression_inner(
        &self,
        expression: &ProjectionExpression,
        catalog: &OptimizerCatalog,
        depth: usize,
    ) -> Option<ScalarFact> {
        if depth > 16 {
            return None;
        }
        if let Some(atom) = Atom::expression(expression) {
            return self.scalar(&atom, catalog);
        }
        match expression {
            ProjectionExpression::Literal(value) => Some(ScalarFact::literal(value)),
            ProjectionExpression::IsNull {
                expression,
                negated,
            } => {
                let input = self.expression_inner(expression, catalog, depth + 1)?;
                let constant = input
                    .constant
                    .as_ref()
                    .map(|value| Value::Bool(matches!(value, Value::Null) != *negated));
                Some(ScalarFact {
                    nonnull: true,
                    domain: Domain::Bool,
                    constant,
                })
            }
            _ => None,
        }
    }

    fn set(&mut self, atom: Atom, fact: ScalarFact) {
        let atom = self.canonical(&atom).clone();
        if self.values.contains_key(&atom) || self.values.len() < MAX_FIELDS {
            self.values.insert(atom, fact);
        }
    }
    pub(super) fn add_key(&mut self, mut key: Vec<Atom>) {
        key.sort();
        key.dedup();
        if !key.is_empty()
            && key.len() <= MAX_KEY_FIELDS
            && self.keys.len() < MAX_KEYS
            && !self.keys.contains(&key)
        {
            self.keys.push(key);
        }
    }
    fn nonnull(&mut self, atom: &Atom, catalog: &OptimizerCatalog) {
        if let Some(mut fact) = self.scalar(atom, catalog) {
            fact.nonnull = true;
            self.set(atom.clone(), fact);
            self.promote_unique(atom, catalog);
        }
    }
    fn promote_unique(&mut self, atom: &Atom, catalog: &OptimizerCatalog) {
        let Atom::Property(variable, property) = atom else {
            return;
        };
        let Some(binding) = self
            .bindings
            .get(variable)
            .filter(|binding| binding.unique_rows)
        else {
            return;
        };
        if !self
            .scalar(atom, catalog)
            .is_some_and(|fact| fact.nonnull && fact.domain != Domain::Unknown)
        {
            return;
        }
        let unique = catalog.source_schema.as_ref().is_some_and(|schema| {
            schema
                .label_id(&binding.label)
                .is_some_and(|label| schema.unique_constraint_id(label, property).is_some())
        });
        if unique {
            self.add_key(vec![atom.clone()]);
        }
    }
    fn equate(&mut self, a: &Atom, b: &Atom, catalog: &OptimizerCatalog) {
        self.nonnull(a, catalog);
        self.nonnull(b, catalog);
        let (Some(left), Some(right)) = (self.scalar(a, catalog), self.scalar(b, catalog)) else {
            return;
        };
        // Keep equivalence propagation in explicitly enforced comparator
        // domains; opaque/mixed numeric domains do not acquire a proof.
        if left.domain == Domain::Unknown || left.domain != right.domain {
            return;
        }
        let a = self.canonical(a).clone();
        let b = self.canonical(b).clone();
        let (root, other) = if a <= b { (a, b) } else { (b, a) };
        if root == other || self.equivalents.len().saturating_add(2) > MAX_FIELDS {
            return;
        }
        for target in self.equivalents.values_mut() {
            if *target == other {
                *target = root.clone();
            }
        }
        self.equivalents.insert(other.clone(), root.clone());
        self.equivalents.insert(root.clone(), root.clone());
        let mut fact = self.scalar(&root, catalog).unwrap_or_default();
        fact.nonnull = true;
        fact.constant = fact.constant.or(left.constant).or(right.constant);
        self.set(root, fact);
    }
    fn equals_literal(&mut self, atom: &Atom, value: &Value, catalog: &OptimizerCatalog) {
        if matches!(value, Value::Null) {
            return;
        }
        self.nonnull(atom, catalog);
        if let Some(mut fact) = self
            .scalar(atom, catalog)
            .filter(|fact| fact.domain.exact_literal(value))
            && let Some(constant) = ScalarFact::literal(value).constant
        {
            fact.constant = Some(constant);
            self.set(atom.clone(), fact);
        }
    }

    pub(super) fn filter(&mut self, predicate: &Predicate, catalog: &OptimizerCatalog) {
        let mut inspect = MAX_FIELDS;
        self.safe &= self.predicate_safe(predicate, catalog, 0, &mut inspect);
        let mut constrain = MAX_FIELDS;
        self.constrain(predicate, catalog, 0, &mut constrain);
        if self.keys.iter().any(|key| {
            key.iter().all(|atom| {
                self.scalar(atom, catalog)
                    .is_some_and(|fact| fact.constant.is_some())
            })
        }) {
            self.max_rows = Some(1);
        }
    }

    fn predicate_safe(
        &self,
        predicate: &Predicate,
        catalog: &OptimizerCatalog,
        depth: usize,
        remaining: &mut usize,
    ) -> bool {
        if depth > 16 || *remaining == 0 {
            return false;
        }
        *remaining -= 1;
        match predicate {
            Predicate::ConstantBool(_) => true,
            Predicate::And(parts) | Predicate::Or(parts) => {
                parts.len() <= 16
                    && parts
                        .iter()
                        .all(|part| self.predicate_safe(part, catalog, depth + 1, remaining))
            }
            Predicate::Not(part) => self.predicate_safe(part, catalog, depth + 1, remaining),
            Predicate::PropertyEq {
                variable, property, ..
            }
            | Predicate::PropertyIsNull { variable, property }
            | Predicate::PropertyIsNotNull { variable, property } => name(variable)
                .zip(name(property))
                .is_some_and(|(variable, property)| {
                    self.scalar(&Atom::Property(variable, property), catalog)
                        .is_some()
                }),
            Predicate::IdEq { variable, .. } => name(variable)
                .is_some_and(|variable| self.scalar(&Atom::Id(variable), catalog).is_some()),
            Predicate::ExpressionEq { expression, value } => {
                self.expression(expression, catalog).is_some()
                    && self.expression(value, catalog).is_some()
            }
            _ => false,
        }
    }
    fn constrain(
        &mut self,
        predicate: &Predicate,
        catalog: &OptimizerCatalog,
        depth: usize,
        remaining: &mut usize,
    ) {
        if depth > 16 || *remaining == 0 {
            return;
        }
        *remaining -= 1;
        match predicate {
            Predicate::And(parts) if parts.len() <= 16 => {
                for part in parts {
                    if *remaining == 0 {
                        break;
                    }
                    self.constrain(part, catalog, depth + 1, remaining);
                }
            }
            Predicate::PropertyEq {
                variable,
                property,
                value,
            } => {
                if let (Some(variable), Some(property)) = (name(variable), name(property)) {
                    self.equals_literal(&Atom::Property(variable, property), value, catalog);
                }
            }
            Predicate::PropertyIsNotNull { variable, property } => {
                if let (Some(variable), Some(property)) = (name(variable), name(property)) {
                    self.nonnull(&Atom::Property(variable, property), catalog);
                }
            }
            Predicate::IdEq { variable, value } => {
                if let Some(variable) = name(variable) {
                    self.equals_literal(&Atom::Id(variable), value, catalog);
                }
            }
            Predicate::ExpressionEq { expression, value } => {
                match (
                    Atom::expression(expression),
                    Atom::expression(value),
                    expression,
                    value,
                ) {
                    (Some(a), Some(b), _, _) => self.equate(&a, &b, catalog),
                    (Some(a), _, _, ProjectionExpression::Literal(value))
                    | (_, Some(a), ProjectionExpression::Literal(value), _) => {
                        self.equals_literal(&a, value, catalog)
                    }
                    (
                        _,
                        _,
                        ProjectionExpression::IsNull {
                            expression,
                            negated,
                        },
                        ProjectionExpression::Literal(Value::Bool(expected)),
                    ) if negated == expected => {
                        if let Some(atom) = Atom::expression(expression) {
                            self.nonnull(&atom, catalog);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    pub(super) fn project(&self, items: &[Projection], catalog: &OptimizerCatalog) -> Self {
        if items.len() > MAX_FIELDS || items.iter().any(|item| item.name.len() > MAX_NAME_BYTES) {
            return Self {
                values_only: self.values_only,
                max_rows: self.max_rows,
                ..Self::default()
            };
        }
        let mut output = Self {
            safe: self.safe && items.len() <= MAX_FIELDS,
            values_only: self.values_only,
            max_rows: self.max_rows,
            ..Self::default()
        };
        let mut names = BTreeSet::new();
        let mut ambiguous = BTreeSet::new();
        for item in items.iter().take(MAX_FIELDS) {
            if !names.insert(item.name.as_str()) {
                ambiguous.insert(item.name.as_str());
            }
        }
        let mut mappings = Vec::new();
        for item in items.iter().take(MAX_FIELDS) {
            let Some(column) = name(&item.name).filter(|_| !ambiguous.contains(item.name.as_str()))
            else {
                output.safe = false;
                continue;
            };
            let Some(fact) = self.expression(&item.expression, catalog) else {
                output.safe = false;
                continue;
            };
            let target = Atom::Column(column);
            output.set(target.clone(), fact);
            if let Some(source) = Atom::expression(&item.expression) {
                mappings.push((source, target));
            }
        }
        let map_atom = |atom: &Atom| {
            mappings
                .iter()
                .find(|(source, _)| {
                    self.equivalent(atom, source) || self.identifies(atom, source, catalog)
                })
                .map(|(_, target)| target.clone())
        };
        for key in &self.keys {
            if let Some(key) = key.iter().map(map_atom).collect::<Option<Vec<_>>>() {
                output.add_key(key);
            }
        }
        // Referenced source constraints are checked by point lookup; never scan
        // the full schema or mistake a duplicated join binding for a unique row.
        for (source, target) in &mappings {
            let Atom::Property(variable, property) = source else {
                continue;
            };
            let unique = self
                .bindings
                .get(variable)
                .filter(|binding| binding.unique_rows)
                .is_some_and(|binding| {
                    catalog.source_schema.as_ref().is_some_and(|schema| {
                        schema.label_id(&binding.label).is_some_and(|label| {
                            schema.unique_constraint_id(label, property).is_some()
                        })
                    })
                });
            if unique
                && self
                    .scalar(source, catalog)
                    .is_some_and(|fact| fact.nonnull && fact.domain != Domain::Unknown)
            {
                output.add_key(vec![target.clone()]);
            }
        }
        for (source, target) in &mappings {
            let canonical = self.canonical(source);
            if let Some((_, representative)) = mappings
                .iter()
                .find(|(candidate, _)| self.canonical(candidate) == canonical)
                && representative != target
            {
                output
                    .equivalents
                    .insert(target.clone(), representative.clone());
            }
        }
        output.ordering = self
            .ordering
            .iter()
            .map(|(atom, direction)| map_atom(atom).map(|mapped| (mapped, *direction)))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        output
    }

    fn identifies(&self, identity: &Atom, candidate: &Atom, catalog: &OptimizerCatalog) -> bool {
        let (Atom::Id(variable), Atom::Property(source, property)) = (identity, candidate) else {
            return false;
        };
        if variable != source {
            return false;
        }
        let Some(binding) = self.bindings.get(variable) else {
            return false;
        };
        self.scalar(candidate, catalog)
            .is_some_and(|fact| fact.nonnull && fact.domain != Domain::Unknown)
            && catalog.source_schema.as_ref().is_some_and(|schema| {
                schema
                    .label_id(&binding.label)
                    .is_some_and(|label| schema.unique_constraint_id(label, property).is_some())
            })
    }

    pub(super) fn distinct(&mut self) {
        if !self.safe {
            return;
        }
        let columns = self
            .values
            .keys()
            .filter(|atom| matches!(atom, Atom::Column(_)))
            .cloned()
            .collect::<Vec<_>>();
        if columns.is_empty() {
            self.max_rows = Some(1);
        } else {
            self.add_key(columns);
        }
    }

    pub(super) fn values_are_unique(&self) -> bool {
        self.max_rows.is_some_and(|max| max <= 1)
            || self.keys.iter().any(|key| {
                key.iter()
                    .all(|atom| matches!(atom, Atom::Column(_)) && self.values.contains_key(atom))
            })
    }
    pub(super) fn grouping_is_unique(&self, keys: &[Projection]) -> bool {
        if keys.len() > MAX_KEY_FIELDS {
            return false;
        }
        let atoms = keys
            .iter()
            .map(|item| Atom::expression(&item.expression))
            .collect::<Option<Vec<_>>>();
        self.max_rows.is_some_and(|max| max <= 1)
            || atoms.is_some_and(|atoms| {
                self.keys.iter().any(|key| {
                    key.iter().all(|atom| {
                        atoms
                            .iter()
                            .any(|candidate| self.equivalent(atom, candidate))
                    })
                })
            })
    }

    pub(super) fn product(mut left: Self, mut right: Self) -> Self {
        if left
            .bindings
            .keys()
            .any(|name| right.bindings.contains_key(name))
            || left.values.keys().any(|key| right.values.contains_key(key))
        {
            return Self::default();
        }
        let mut result = Self {
            safe: left.safe
                && right.safe
                && left.values.len().saturating_add(right.values.len()) <= MAX_FIELDS
                && left.bindings.len().saturating_add(right.bindings.len()) <= MAX_FIELDS,
            values_only: left.values_only && right.values_only,
            max_rows: left
                .max_rows
                .zip(right.max_rows)
                .and_then(|(a, b)| a.checked_mul(b)),
            ..Self::default()
        };
        for a in &left.keys {
            for b in &right.keys {
                result.add_key(a.iter().chain(b).cloned().collect());
            }
        }
        if !right.max_rows.is_some_and(|max| max <= 1) {
            for binding in left.bindings.values_mut() {
                binding.unique_rows = false;
            }
        }
        if !left.max_rows.is_some_and(|max| max <= 1) {
            for binding in right.bindings.values_mut() {
                binding.unique_rows = false;
            }
        }
        if left.bindings.len() + right.bindings.len() <= MAX_FIELDS {
            result.bindings = left.bindings;
            result.bindings.extend(right.bindings);
        }
        for (atom, fact) in left.values.into_iter().chain(right.values) {
            result.set(atom, fact);
        }
        // Keys above retain both input identities; cross-input equivalence is
        // introduced only by a later checked equality filter.
        result
    }

    pub(super) fn set_group_key(&mut self, keys: &[Projection]) {
        if keys.is_empty() {
            self.max_rows = Some(1);
            return;
        }
        if keys.len() > MAX_KEY_FIELDS {
            return;
        }
        let atoms = keys
            .iter()
            .filter_map(|key| name(&key.name).map(Atom::Column))
            .collect::<Vec<_>>();
        if atoms.len() == keys.len() && keys.len() <= MAX_KEY_FIELDS {
            self.add_key(atoms);
        }
    }
    pub(super) fn result_column(&mut self, name_value: &str, count: bool) {
        if let Some(name) = name(name_value) {
            self.set(
                Atom::Column(name),
                ScalarFact {
                    nonnull: count,
                    domain: if count { Domain::Int } else { Domain::Unknown },
                    constant: None,
                },
            );
        } else {
            self.safe = false;
        }
    }
}
