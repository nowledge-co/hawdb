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
use crate::relational::{
    relational_index_recovery_delta_file, RelationalIndexRecoveryBuilder,
    RelationalIndexRecoveryConfig, RelationalRecoveryFence, RelationalRecoverySourceIdentity,
};
use std::io::Write;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Exact,
    Range,
    Batch,
}

fn recovered_fixture() -> (Fixture, RelationalIndexReadView, PathBuf) {
    let (fixture, base, mut oracle, mut state) = Fixture::open();
    let capture = replace(&mut state, &mut oracle, 0, None);
    let config = RelationalIndexRecoveryConfig::default();
    let mut builder = RelationalIndexRecoveryBuilder::new(&fixture.0, 1, 40, config).unwrap();
    builder.record(41, capture).unwrap();
    builder.finish(41).unwrap();
    let reader = RelationalIndexRecoveryReader::open_latest(
        &fixture.0,
        RelationalRecoveryFence::new(41, RelationalRecoverySourceIdentity::for_test(40, 41)),
        RelationalIndexShadowConfig::default(),
        config,
    )
    .unwrap();
    let manifest = reader.manifest();
    assert_eq!(manifest.delta_pages(), 1);
    let path = fixture.0.join(relational_index_recovery_delta_file(
        manifest.base_generation,
        manifest.delta_generation,
        0,
    ));
    drop(base);
    (
        fixture,
        RelationalIndexReadView::from_recovered(reader),
        path,
    )
}

fn visit(
    view: &RelationalIndexReadView,
    operation: Operation,
    limits: RelationalIndexReadLimits,
) -> (
    Result<RelationalIndexReadViewReport, RelationalIndexShadowError>,
    usize,
) {
    let mut rows = 0;
    let result = match operation {
        Operation::Exact => view.visit_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits, |_| {
            rows += 1;
            true
        }),
        Operation::Range => view.visit_range_entries(
            TABLE,
            INDEX,
            &RelationalIndexRangeScan {
                prefix: key(&[0]),
                exclusive_bound: None,
                direction: RelationalIndexScanDirection::Forward,
            },
            limits,
            |_, _| {
                rows += 1;
                true
            },
        ),
        Operation::Batch => {
            view.visit_prefix_entries_many(TABLE, INDEX, &[key(&[0, 0])], limits, |_, _| {
                rows += 1;
                true
            })
        }
    };
    (result, rows)
}

#[test]
fn recovery_payload_append_fails_closed_for_all_selected_read_operations() {
    for operation in [Operation::Exact, Operation::Range, Operation::Batch] {
        let (_fixture, view, path) = recovered_fixture();
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(&[0x39])
            .unwrap();

        let (result, rows) = visit(&view, operation, RelationalIndexReadLimits::default());
        assert!(
            matches!(result, Err(RelationalIndexShadowError::Corrupt(_))),
            "{operation:?}: {result:?}"
        );
        assert_eq!(rows, 0, "no selected result after length drift");
        assert!(view.is_poisoned());
        assert!(matches!(
            visit(&view, operation, RelationalIndexReadLimits::default()).0,
            Err(RelationalIndexShadowError::Corrupt(_))
        ));
    }
}

#[test]
fn recovery_payload_budget_rejection_keeps_the_selected_view_healthy() {
    for operation in [Operation::Exact, Operation::Range, Operation::Batch] {
        let (_fixture, view, path) = recovered_fixture();
        let length = usize::try_from(std::fs::metadata(path).unwrap().len()).unwrap();
        let limits = RelationalIndexReadLimits {
            max_file_bytes: length - 1,
            ..RelationalIndexReadLimits::default()
        };

        let (result, _) = visit(&view, operation, limits);
        assert!(
            matches!(result, Err(RelationalIndexShadowError::Admission(_))),
            "{operation:?}: {result:?}"
        );
        assert!(!view.is_poisoned());
        let (result, rows) = visit(&view, operation, RelationalIndexReadLimits::default());
        assert!(!result.unwrap().stopped_early);
        assert_eq!(
            rows,
            if matches!(operation, Operation::Range) {
                2
            } else {
                0
            }
        );
    }
}
