// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use query_workloads::{generator::StateAwareCaseGenerator, sql::generate_sql_case};

// Captured from the original Shiro generators at 37c0c50ec, before extraction.
// All query/oracle/setup fields participate, including typed parameters and ASTs.
#[test]
fn legacy_seed_inventory_is_unchanged() {
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    let mut observe = |text: String| {
        for byte in text.bytes() {
            digest = (digest ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
        }
    };
    for seed in [0, 1, 7, 42, u64::MAX] {
        let mut generator = StateAwareCaseGenerator::new(seed);
        for index in 0..144 {
            let case = generator.case(index);
            observe(format!("{case:?}\n"));
            for indexed in [false, true] {
                observe(format!(
                    "{:?}\n",
                    generate_sql_case(case.seed, index, indexed)
                ));
            }
        }
    }
    assert_eq!(digest, 0x99dfc326a44f0002);
}
