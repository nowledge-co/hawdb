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

package hawdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// ffiBuffer mirrors HawdbBuffer in the C header: {char *ptr; uintptr_t len}.
// Every buffer the library fills through one is caller-owned and freed with
// hawdb_buffer_free.
type ffiBuffer struct {
	ptr *byte
	len uintptr
}

// Library is a loaded libhawdb_ffi shared library with its exported
// functions registered. A Library is safe for concurrent use and is never
// unloaded: Go frames may return into the shared library's code, so the
// mapping has to live for the process lifetime.
type Library struct {
	open       func(path *byte, pathLen uintptr, options *byte, optionsLen uintptr, errOut *ffiBuffer) unsafe.Pointer
	close      func(db unsafe.Pointer)
	query      func(db unsafe.Pointer, text *byte, textLen uintptr, params *byte, paramsLen uintptr, result, errOut *ffiBuffer) bool
	querySQL   func(db unsafe.Pointer, text *byte, textLen uintptr, params *byte, paramsLen uintptr, result, errOut *ffiBuffer) bool
	bufferFree func(buffer *ffiBuffer)
	version    func(out *ffiBuffer)
	openMemory func(errOut *ffiBuffer) unsafe.Pointer
}

// LoadLibrary loads libhawdb_ffi from path. Missing libraries and
// missing or incompatible exported symbols fail loudly here, before any
// database is opened.
func LoadLibrary(path string) (*Library, error) {
	handle, err := loadSharedLibrary(path)
	if err != nil {
		return nil, fmt.Errorf("hawdb: cannot load %s: %w", path, err)
	}
	lib := &Library{}
	purego.RegisterLibFunc(&lib.open, handle, "hawdb_open")
	purego.RegisterLibFunc(&lib.close, handle, "hawdb_close")
	purego.RegisterLibFunc(&lib.query, handle, "hawdb_query")
	purego.RegisterLibFunc(&lib.querySQL, handle, "hawdb_query_sql")
	purego.RegisterLibFunc(&lib.bufferFree, handle, "hawdb_buffer_free")
	purego.RegisterLibFunc(&lib.version, handle, "hawdb_version")
	// Additive capability: older libraries remain usable for persistent stores.
	if symbol, err := lookupSharedSymbol(handle, "hawdb_open_in_memory"); err == nil {
		purego.RegisterFunc(&lib.openMemory, symbol)
	}
	return lib, nil
}

// defaultLibrary resolves the process-wide library used by the package-level
// Open and Version: HAWDB_LIBRARY wins, then the platform filename next to
// the working directory and the executable, falling back to the bare
// filename for the OS loader's own search paths.
var defaultLibrary = sync.OnceValues(func() (*Library, error) {
	path, err := findLibrary()
	if err != nil {
		return nil, err
	}
	return LoadLibrary(path)
})

func findLibrary() (string, error) {
	if path := os.Getenv("HAWDB_LIBRARY"); path != "" {
		return path, nil
	}
	name := libraryName()
	if cwd, err := os.Getwd(); err == nil {
		candidate := filepath.Join(cwd, name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return name, nil
}

// strArg passes s to C as a (ptr, len) pair. An empty string becomes
// (NULL, 0), which the FFI treats as absent for optional arguments.
func strArg(s string) (*byte, uintptr) {
	if s == "" {
		return nil, 0
	}
	return unsafe.StringData(s), uintptr(len(s))
}

// takeBytes copies a buffer's content and releases the buffer.
func (lib *Library) takeBytes(buffer *ffiBuffer) []byte {
	if buffer.ptr == nil {
		return nil
	}
	defer lib.bufferFree(buffer)
	return bytes.Clone(unsafe.Slice(buffer.ptr, buffer.len))
}

// takeErr consumes an error buffer filled by a failed call. The library
// delivers errors as {"kind", "message"} JSON; an unparseable payload
// degrades to the raw message.
func (lib *Library) takeErr(buffer *ffiBuffer) error {
	data := lib.takeBytes(buffer)
	if data == nil {
		return nil
	}
	var parsed struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &parsed); err == nil && parsed.Message != "" {
		return &Error{Kind: parsed.Kind, Message: parsed.Message}
	}
	return &Error{Message: string(data)}
}
