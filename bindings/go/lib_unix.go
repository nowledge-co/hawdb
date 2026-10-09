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

//go:build !windows

package hawdb

import (
	"runtime"

	"github.com/ebitengine/purego"
)

func libraryName() string {
	if runtime.GOOS == "darwin" {
		return "libhawdb_ffi.dylib"
	}
	return "libhawdb_ffi.so"
}

func loadSharedLibrary(path string) (uintptr, error) {
	// The library is deliberately never dlclosed: registered Go function
	// pointers outlive any handle teardown, and unloading could leave a Go
	// frame returning into unmapped code.
	return purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
}
