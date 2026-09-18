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

//! Test-only protocol models consumed by the root facade's inline tests.

#[path = "analytics.rs"]
pub mod analytics;
#[path = "graph_read.rs"]
pub mod graph_read;
#[path = "lifecycle.rs"]
pub mod lifecycle;
#[path = "mutation.rs"]
pub mod mutation;
