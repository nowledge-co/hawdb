// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Complete external checkpoint contracts, grouped by facade feature profile.

#![cfg(not(target_arch = "wasm32"))]

#[path = "checkpoint/manual.rs"]
mod manual_checkpoint;

#[path = "checkpoint/wal_recovery.rs"]
mod wal_recovery;
