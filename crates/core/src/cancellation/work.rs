// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(super) struct RuntimeWorkBudget {
    limit: usize,
    used: AtomicUsize,
    parents: [Option<Arc<Self>>; 2],
    has_join: bool,
    refusal: Mutex<Option<ResourceBudgetError>>,
}

impl RuntimeWorkBudget {
    pub(super) fn new(limit: usize, parent: Option<Arc<Self>>) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            has_join: parent.as_ref().is_some_and(|parent| parent.has_join),
            parents: [parent, None],
            refusal: Mutex::new(None),
        }
    }

    pub(super) fn intersect(
        left: &Option<Arc<Self>>,
        right: &Option<Arc<Self>>,
    ) -> Option<Arc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) if left.contains(right) => Some(left.clone()),
            (Some(left), Some(right)) if right.contains(left) => Some(right.clone()),
            (Some(left), Some(right)) if !Arc::ptr_eq(left, right) => Some(Arc::new(Self {
                limit: usize::MAX,
                used: AtomicUsize::new(0),
                parents: [Some(left.clone()), Some(right.clone())],
                has_join: true,
                refusal: Mutex::new(None),
            })),
            (Some(left), _) => Some(left.clone()),
            (_, right) => right.clone(),
        }
    }

    fn contains(&self, ancestor: &Arc<Self>) -> bool {
        if self.has_join {
            return std::ptr::eq(self, Arc::as_ptr(ancestor));
        }
        let mut current = Some(self);
        while let Some(scope) = current {
            if std::ptr::eq(scope, Arc::as_ptr(ancestor)) {
                return true;
            }
            current = scope.parents[0].as_deref();
        }
        false
    }

    pub(super) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub(super) fn refusal(&self) -> Option<ResourceBudgetError> {
        self.refusal
            .lock()
            .expect("work refusal lock")
            .clone()
            .or_else(|| {
                self.parents
                    .iter()
                    .flatten()
                    .find_map(|parent| parent.refusal())
            })
    }

    pub(super) fn charge(&self, units: usize) -> Result<()> {
        if self.has_join {
            self.charge_once(units, &mut std::collections::BTreeSet::new())
        } else {
            self.charge_local(units)?;
            if let Some(parent) = &self.parents[0] {
                parent.charge(units)?;
            }
            Ok(())
        }
    }

    fn charge_once(
        &self,
        units: usize,
        visited: &mut std::collections::BTreeSet<*const Self>,
    ) -> Result<()> {
        if !visited.insert(std::ptr::from_ref(self)) {
            return Ok(());
        }
        self.charge_local(units)?;
        for parent in self.parents.iter().flatten() {
            parent.charge_once(units, visited)?;
        }
        Ok(())
    }
    fn charge_local(&self, units: usize) -> Result<()> {
        // A parent's refusal may leave a conservative child charge, never an
        // undercharge. Query failure discards the complete scoped result.
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(units).filter(|next| *next <= self.limit)
            })
            .map_err(|used| {
                let error = ResourceBudgetError {
                    resource: ResourceBudgetKind::WorkUnits,
                    scope: ResourceBudgetScope::Operator,
                    owner: "bounded query input".into(),
                    memory_class: None,
                    requested: units as u64,
                    used: used as u64,
                    budget: self.limit as u64,
                    budget_option: "RuntimeTaskContext.work_limit",
                    message: "query input exceeds its work limit; no partial domain is returned"
                        .into(),
                };
                let mut refusal = self.refusal.lock().expect("work refusal lock");
                if refusal.is_none() {
                    *refusal = Some(error.clone());
                }
                error
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{HawDBError, RuntimeTaskContext};

    #[test]
    fn shared_work_limit_preserves_parent_and_failed_charge() {
        let parent = RuntimeTaskContext::default().with_work_limit(3);
        let child = parent.clone().with_work_limit(2);
        child.child().charge_work(2).unwrap();
        assert_eq!(child.work_units(), 2);
        assert_eq!(parent.work_units(), 2);
        assert!(matches!(
            child.charge_work(1),
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        assert_eq!(parent.work_units(), 2);
        assert!(matches!(
            child.check_work_limit(),
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        assert!(parent.check_work_limit().is_ok());
        parent.charge_work(1).unwrap();
        let another = parent.clone().with_work_limit(10);
        assert!(another.charge_work(1).is_err());
        assert_eq!(parent.work_units(), 3);
    }

    #[test]
    fn unbounded_work_charge_leaves_cancellation_to_checkpoints() {
        let task = RuntimeTaskContext::default();
        task.cancellation().cancel();
        task.charge_work(1).unwrap();
        assert_eq!(task.work_units(), 0);
        assert!(task.checkpoint().is_err());
    }

    #[test]
    fn work_charge_observes_original_cancellation() {
        let task = RuntimeTaskContext::default().with_work_limit(1);
        let child = task.child();
        task.cancellation().cancel();
        assert!(child.charge_work(1).is_err());
        assert_eq!(task.work_units(), 0);
    }

    #[test]
    fn joined_work_scopes_charge_shared_ancestors_once() {
        let parent = RuntimeTaskContext::default().with_work_limit(3);
        let request = parent.child().with_work_limit(2);
        let combined = parent.constrained_by(&request).unwrap();
        combined.charge_work(2).unwrap();
        assert_eq!(parent.work_units(), 2);
        assert_eq!(request.work_units(), 2);
        assert!(combined.charge_work(1).is_err());
        assert!(combined.check_work_limit().is_err());
        assert!(parent.work_units() <= 3);
    }

    #[test]
    fn joined_independent_work_scopes_preserve_both_limits() {
        let parent = RuntimeTaskContext::default().with_work_limit(3);
        let request = RuntimeTaskContext::default().with_work_limit(2);
        let combined = parent.constrained_by(&request).unwrap();
        combined.charge_work(2).unwrap();
        assert_eq!(parent.work_units(), 2);
        assert_eq!(request.work_units(), 2);
        assert!(combined.charge_work(1).is_err());
        assert!(combined.check_work_limit().is_err());
    }
}
