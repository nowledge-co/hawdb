// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use query_workloads::{generator, sql};
fn main() {
    for seed in [0, 1, 7, 42, u64::MAX] {
        let mut g = generator::StateAwareCaseGenerator::new(seed);
        for index in 0..144 {
            let case = g.case(index);
            println!("{case:?}");
            for indexed in [false, true] {
                println!("{:?}", sql::generate_sql_case(case.seed, index, indexed));
            }
        }
    }
}
