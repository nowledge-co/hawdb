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

//go:build bazel

package hawdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
)

// Resolve the declared library before any tests run, including on platforms
// whose runfiles are represented by a manifest rather than a symlink tree.
func TestMain(m *testing.M) {
	path, err := runfiles.Rlocation(os.Getenv("HAWDB_LIBRARY"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve Bazel FFI library: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("HAWDB_LIBRARY", path); err != nil {
		fmt.Fprintf(os.Stderr, "set Bazel FFI library: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
