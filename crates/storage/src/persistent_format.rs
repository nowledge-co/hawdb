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

//! The engine-wide persistent format version.
//!
//! A database directory is written in many independently versioned codecs
//! (manifests, WAL envelopes, row pages, segments; see docs/STORAGE.md).
//! Each codec refuses versions it does not support. This single integer
//! summarizes all of them for embedding hosts: two HawDB releases with the
//! same value read each other's databases; a different value means the host
//! must carry its data across with a logical dump (docs/LOGICAL_DUMP.md).
//!
//! Bump it in the same change as any persistent codec, magic, version or
//! layout change that makes an existing database unreadable, and regenerate
//! `docs/persistent-format.lock` with
//! `python3 scripts/check-persistent-format.py --update`. CI fails while the
//! lock and the persistent codec constants disagree.

/// Engine-wide persistent format version (docs/STORAGE.md, "Persistent
/// format version").
///
/// Version 3 (#1737): checkpoints no longer write the legacy full-row
/// `relational.N.hawdb` file; the canonical row-page and overflow roots carry
/// the relational state. Version 3 reads version 2 databases. Version 2
/// releases open version 3 databases only with OutOfCore residency and
/// Authoritative relational indexes and refuse every other configuration.
pub const PERSISTENT_FORMAT_VERSION: u32 = 3;
