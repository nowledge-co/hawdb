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

use super::*;

#[test]
fn metadata_count_fallback_preserves_planning_purpose_and_mode_authority() {
    for (outcome, reason) in [
        (Outcome::Admission, "metadata_count_admission_rejected"),
        (Outcome::Missing, "metadata_count_missing_index"),
    ] {
        let reader = Reader::script(outcome, false);
        for authoritative in [false, true] {
            let runtime = RelationalIndexRuntime::new(
                if authoritative {
                    RelationalIndexReadMode::Authoritative(&reader)
                } else {
                    RelationalIndexReadMode::DemandPaged(&reader)
                },
                Default::default(),
            );
            let result = runtime.exact_posting_count(TABLE, INDEX, &key(&[0, 0]));
            if authoritative {
                assert!(result.is_err(), "{outcome:?}: {result:?}");
                assert!(runtime.evidence().is_empty());
            } else {
                assert_eq!(result.unwrap(), None);
                let evidence = runtime.evidence();
                assert_eq!(evidence.len(), 1);
                assert_eq!(evidence[0].lookups, 1);
                assert_eq!(evidence[0].metadata_count_lookups, 1);
                assert_eq!(evidence[0].canonical_fallback_lookups, 0);
                assert_eq!(evidence[0].demand_paged_lookups, 0);
                assert_eq!(evidence[0].authoritative_lookups, 0);
                assert_eq!(evidence[0].fallback_reasons, BTreeSet::from([reason]));
            }
        }
    }
}

#[test]
fn metadata_count_integrity_errors_fail_closed_in_every_mode() {
    for outcome in [Outcome::Corrupt, Outcome::Durability, Outcome::Stale] {
        let reader = Reader::script(outcome, false);
        for mode in [
            RelationalIndexReadMode::DemandPaged(&reader),
            RelationalIndexReadMode::Authoritative(&reader),
        ] {
            let runtime = RelationalIndexRuntime::new(mode, Default::default());
            assert!(matches!(
                runtime.exact_posting_count(TABLE, INDEX, &key(&[0, 0])),
                Err(HawDBError::StorageIntegrity(_))
            ));
            assert!(runtime.evidence().is_empty());
        }
    }
}

/// A persistent provider that implements only the original report-based seam.
struct LegacyOnlyReader<'a>(&'a Reader);

impl RelationalIndexStoreReader for LegacyOnlyReader<'_> {
    fn relational_index_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        self.0
            .relational_index_probe_statistics(table, index, prefix_len)
    }

    fn visit_relational_index_read_view_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        self.0
            .visit_relational_index_read_view_prefix_entries(table, index, prefix, limits, visit)
    }

    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        self.0.visit_relational_index_read_view_prefix_entries_many(
            table, index, prefixes, limits, visit,
        )
    }

    fn visit_relational_index_read_view_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        self.0
            .visit_relational_index_read_view_range_entries(table, index, scan, limits, visit)
    }
}

#[test]
fn legacy_provider_declines_before_reading_and_preserves_mode_contracts() {
    let fixture = Fixture::new();
    let provider = LegacyOnlyReader(&fixture.reader);
    for authoritative in [false, true] {
        for kind in 0..3 {
            let runtime = RelationalIndexRuntime::new(
                if authoritative {
                    RelationalIndexReadMode::Authoritative(&provider)
                } else {
                    RelationalIndexReadMode::DemandPaged(&provider)
                },
                Default::default(),
            );
            let mut rows = Vec::new();
            let result = visit(&runtime, &fixture.state, kind, |index, primary| {
                rows.push((index.clone(), primary.clone()));
                Ok(true)
            });
            if authoritative {
                assert!(matches!(result, Err(HawDBError::StorageIntegrity(_))));
                assert!(rows.is_empty());
                assert!(runtime.evidence().is_empty());
            } else {
                assert!(result.unwrap());
                assert_eq!(rows, expected(&fixture.oracle, &scan(&[1], false, None)));
                let evidence = runtime.evidence();
                assert_eq!(evidence.len(), 1);
                assert_eq!(evidence[0].canonical_fallback_lookups, 1);
                assert_eq!(
                    evidence[0].fallback_reasons,
                    BTreeSet::from(["read_view_unavailable"])
                );
            }
            assert!(fixture.reader.limits.borrow().is_empty());
            assert!(fixture.reader.batches.borrow().is_empty());
            assert_eq!(runtime.remaining_limits().unwrap(), runtime.limits);
            assert!(!fixture.reader.view().is_poisoned());
        }
    }
    fixture.remove();
}
