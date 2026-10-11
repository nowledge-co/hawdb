// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[path = "host_boundary/workload.rs"]
mod workload;

fn main() {
    workload::main_with_observer(None);
}
