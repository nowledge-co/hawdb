// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Prove complete, unique mappings before exposing an eligible ANN allowlist.

use crate::out_of_core::SearchMetadataDocument;
use hawdb_core::{HawDBError, Result};
use hawdb_executor::{
    vector_eligibility::VectorEligibleDomain, QueryMemoryAccount, QueryMemoryLease,
};
use std::collections::BTreeSet;

pub(super) struct EligibleMapping<'a> {
    domain: &'a VectorEligibleDomain<'a>,
    // Borrow canonical keys; retained native candidate blocks own their copies.
    matched: BTreeSet<&'a str>,
    memory: QueryMemoryLease,
}

impl<'a> EligibleMapping<'a> {
    pub(super) fn new(
        domain: &'a VectorEligibleDomain<'a>,
        account: &QueryMemoryAccount,
    ) -> Result<Self> {
        Ok(Self {
            domain,
            matched: BTreeSet::new(),
            memory: account.reserve(std::mem::size_of::<Self>())?,
        })
    }

    pub(super) fn charge_visit(&self) -> Result<()> {
        self.domain.task().charge_work(1)
    }

    pub(super) fn admit(&mut self, document: &SearchMetadataDocument) -> Result<bool> {
        let Some(identity) = self.domain.find_identity(&document.id) else {
            return Ok(false);
        };
        if document.vector_ordinal.is_none() {
            return Err(HawDBError::StorageIntegrity(
                "eligible vector has no ordinal in the pinned index".into(),
            ));
        }
        if self.matched.contains(identity) {
            return Err(HawDBError::StorageIntegrity(
                "eligible vector has multiple visible mappings in the pinned index".into(),
            ));
        }
        self.memory.grow(256)?;
        self.matched.insert(identity);
        Ok(true)
    }

    pub(super) fn finish(self) -> Result<()> {
        self.domain.task().check_work_limit()?;
        if self.matched.len() != self.domain.report().eligible_vectors {
            return Err(HawDBError::StorageIntegrity(
                "eligible vector mapping is incomplete in the pinned index".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::{
        Vector, VectorDimension, VectorDistance, VectorIndexBackend, VectorIndexDescriptor,
        VectorIndexQueryMode, VectorIndexReadIdentity, VectorIndexTarget,
    };
    use std::num::NonZeroU64;

    #[test]
    fn eligible_mapping_proves_completeness_and_refunds_every_exit() {
        super::super::tests::with_query(|_, _, memory, context, _| {
            let definition = VectorIndexDescriptor::new(
                "vectors".into(),
                VectorIndexTarget::Relational {
                    table: "vectors".into(),
                    column: "embedding".into(),
                    column_ordinal: 1,
                },
                VectorDimension::new(2).unwrap(),
                VectorDistance::Cosine,
                VectorIndexBackend::RaBitQ {
                    bit_width: 4,
                    segment_rows: 8,
                    transform_seed: 1,
                },
                [0; 32],
            )
            .unwrap();
            let uuid = hawdb_core::generate_uuidv7().unwrap();
            let identity = VectorIndexReadIdentity {
                database: uuid,
                branch: uuid,
                consumer_id_hash: [0; 32],
                target_locator_hash: [0; 32],
                registration: uuid,
                projection: uuid,
                checkpoint: uuid,
                target_incarnation: NonZeroU64::MIN,
                generation: NonZeroU64::MIN,
                operation: NonZeroU64::MIN,
                source_epoch: 1,
            };
            let vector = Vector::new(&[1.0, 0.0]).unwrap();
            let document = |id: &str, ordinal| SearchMetadataDocument {
                id: id.into(),
                vector_ordinal: ordinal,
                metadata: Default::default(),
            };
            for case in ["complete", "missing", "duplicate", "null", "work", "memory"] {
                let baseline = memory.ledger.snapshot().used_bytes;
                {
                    let mut domain = VectorEligibleDomain::new(
                        &definition,
                        identity,
                        vector.as_ref(),
                        VectorIndexQueryMode::FilteredAnn,
                        if case == "work" { 3 } else { 10 },
                        context.memory,
                        Some(context.task),
                    )
                    .unwrap();
                    for id in ["a", "b"] {
                        domain.observe_input().unwrap();
                        domain.insert(id, vector.as_ref()).unwrap();
                    }
                    domain.finish().unwrap();
                    if case == "memory" {
                        let retained = memory.ledger.snapshot().used_bytes;
                        let blocked = context
                            .memory
                            .reserve(context.memory.available_bytes())
                            .unwrap();
                        let error = EligibleMapping::new(&domain, context.memory)
                            .err()
                            .expect("mapping admission must refuse before allocation");
                        assert!(
                            matches!(error, HawDBError::ResourceBudgetExceeded(ref error) if error.resource == hawdb_core::ResourceBudgetKind::MemoryBytes),
                            "{error:?}"
                        );
                        drop(blocked);
                        assert_eq!(memory.ledger.snapshot().used_bytes, retained);
                    }
                    let mut mapping = EligibleMapping::new(&domain, context.memory).unwrap();
                    mapping.charge_visit().unwrap();
                    assert!(!mapping.admit(&document("outside", Some(2))).unwrap());
                    assert_eq!(domain.task().work_units(), 3);
                    if case == "work" {
                        let error = mapping.charge_visit().unwrap_err();
                        assert!(
                            matches!(error, HawDBError::ResourceBudgetExceeded(ref error) if error.resource == hawdb_core::ResourceBudgetKind::WorkUnits)
                        );
                        assert!(
                            mapping.charge_visit().is_err(),
                            "work refusal must remain sticky"
                        );
                    } else if case == "null" {
                        mapping.charge_visit().unwrap();
                        assert!(matches!(
                            mapping.admit(&document("a", None)),
                            Err(HawDBError::StorageIntegrity(_))
                        ));
                    } else {
                        if case == "memory" {
                            let retained = memory.ledger.snapshot().used_bytes;
                            let blocked = context
                                .memory
                                .reserve(context.memory.available_bytes())
                                .unwrap();
                            let error = mapping.admit(&document("a", Some(0))).unwrap_err();
                            assert!(
                                matches!(error, HawDBError::ResourceBudgetExceeded(ref error) if error.resource == hawdb_core::ResourceBudgetKind::MemoryBytes),
                                "{error:?}"
                            );
                            drop(blocked);
                            assert_eq!(memory.ledger.snapshot().used_bytes, retained);
                        }
                        mapping.charge_visit().unwrap();
                        assert!(mapping.admit(&document("a", Some(0))).unwrap());
                        match case {
                            "complete" | "memory" => {
                                mapping.charge_visit().unwrap();
                                assert!(mapping.admit(&document("b", Some(1))).unwrap());
                                mapping.finish().unwrap();
                            }
                            "missing" => assert!(matches!(
                                mapping.finish(),
                                Err(HawDBError::StorageIntegrity(_))
                            )),
                            "duplicate" => {
                                mapping.charge_visit().unwrap();
                                assert!(matches!(
                                    mapping.admit(&document("a", Some(0))),
                                    Err(HawDBError::StorageIntegrity(_))
                                ));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                assert_eq!(memory.ledger.snapshot().used_bytes, baseline, "{case}");
            }
        });
    }
}
