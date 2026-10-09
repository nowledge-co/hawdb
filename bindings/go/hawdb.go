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

// Package hawdb is a native Go binding for the HawDB embedded database.
//
// It calls libhawdb_ffi (the C ABI built from bindings/ffi) through purego,
// so it works with CGO_ENABLED=0 and needs no cgo toolchain. The shared
// library can be loaded explicitly with LoadLibrary; the package-level
// Open and Version lazily resolve a process-wide default from the
// HAWDB_LIBRARY environment variable, then the platform filename next to
// the working directory and the executable.
package hawdb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// DB is an open HawDB database.
type DB struct {
	lib    *Library
	ptr    unsafe.Pointer
	mu     sync.RWMutex
	closed bool
}

// Error is the error type returned by this package. Kind carries the
// engine's error kind in snake_case (e.g. "capability_unavailable",
// "admission", "stopped"); "invalid_argument" marks a caller-side
// contract violation and "panic" a contained Rust panic.
type Error struct {
	Kind    string
	Message string
}

func (e *Error) Error() string {
	if e.Kind == "" {
		return e.Message
	}
	return e.Kind + ": " + e.Message
}

// OpenOptions controls how a database is opened.
type OpenOptions struct {
	// ReadOnly opens an existing database without permitting writes.
	ReadOnly bool
}

// Result is one materialized statement result: Columns in declaration
// order, and Rows as positional values aligned with Columns.
type Result struct {
	Columns []string
	Rows    [][]any
}

// Dicts returns the rows as maps keyed by column name.
func (r *Result) Dicts() []map[string]any {
	out := make([]map[string]any, len(r.Rows))
	for i, row := range r.Rows {
		m := make(map[string]any, len(r.Columns))
		for j, column := range r.Columns {
			if j < len(row) {
				m[column] = row[j]
			}
		}
		out[i] = m
	}
	return out
}

// Open opens a HawDB database at path on the default library, creating it
// if needed. Pass an OpenOptions to override defaults, e.g.
// Open(path, &OpenOptions{ReadOnly: true}).
func Open(path string, opts ...*OpenOptions) (*DB, error) {
	lib, err := defaultLibrary()
	if err != nil {
		return nil, err
	}
	return lib.Open(path, opts...)
}

// OpenReadOnly opens an existing database on the default library without
// permitting writes.
func OpenReadOnly(path string) (*DB, error) {
	return Open(path, &OpenOptions{ReadOnly: true})
}

// Open opens a HawDB database at path on this library.
func (lib *Library) Open(path string, opts ...*OpenOptions) (*DB, error) {
	var optionsJSON string
	if len(opts) > 0 && opts[0] != nil {
		data, err := json.Marshal(map[string]any{"read_only": opts[0].ReadOnly})
		if err != nil {
			return nil, err
		}
		optionsJSON = string(data)
	}
	var errBuf ffiBuffer
	pathPtr, pathLen := strArg(path)
	optionsPtr, optionsLen := strArg(optionsJSON)
	ptr := lib.open(pathPtr, pathLen, optionsPtr, optionsLen, &errBuf)
	if ptr == nil {
		if err := lib.takeErr(&errBuf); err != nil {
			return nil, err
		}
		return nil, errors.New("hawdb: open failed")
	}
	db := &DB{lib: lib, ptr: ptr}
	// Safety net for callers that forget Close; Close is idempotent.
	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// OpenReadOnly opens an existing database on this library without
// permitting writes.
func (lib *Library) OpenReadOnly(path string) (*DB, error) {
	return lib.Open(path, &OpenOptions{ReadOnly: true})
}

// Query runs one Cypher statement. params maps $name parameters to values;
// supported types are nil, bool, integers, floats, string, []byte (stored
// as binary), []any and map[string]any.
func (db *DB) Query(cypher string, params map[string]any) (*Result, error) {
	return db.query(false, cypher, params)
}

// QuerySQL runs one SQL statement with positional parameters.
func (db *DB) QuerySQL(sql string, params ...any) (*Result, error) {
	return db.query(true, sql, params)
}

// Exec runs a statement that returns no interesting rows (CREATE, MERGE,
// SET, DDL), like database/sql's Exec.
func (db *DB) Exec(cypher string, params map[string]any) error {
	_, err := db.Query(cypher, params)
	return err
}

func (db *DB) query(sql bool, text string, params any) (*Result, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, &Error{Message: "hawdb: database is closed"}
	}
	var paramsJSON string
	if params != nil {
		data, err := json.Marshal(prepareValue(params))
		if err != nil {
			return nil, fmt.Errorf("hawdb: cannot marshal parameters: %w", err)
		}
		// A nil map or slice marshals to "null", not an object or array:
		// treat it as no parameters at all.
		if string(data) != "null" {
			paramsJSON = string(data)
		}
	}
	textPtr, textLen := strArg(text)
	paramsPtr, paramsLen := strArg(paramsJSON)
	var resultBuf, errBuf ffiBuffer
	var ok bool
	if sql {
		ok = db.lib.querySQL(db.ptr, textPtr, textLen, paramsPtr, paramsLen, &resultBuf, &errBuf)
	} else {
		ok = db.lib.query(db.ptr, textPtr, textLen, paramsPtr, paramsLen, &resultBuf, &errBuf)
	}
	if !ok {
		if err := db.lib.takeErr(&errBuf); err != nil {
			return nil, err
		}
		return nil, errors.New("hawdb: query failed")
	}
	var result Result
	decoder := json.NewDecoder(bytes.NewReader(db.lib.takeBytes(&resultBuf)))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("hawdb: malformed result JSON: %w", err)
	}
	for _, row := range result.Rows {
		for i, value := range row {
			row[i] = normalizeValue(value)
		}
	}
	return &result, nil
}

// UUID is a HawDB uuid value for parameters and results. It is the
// canonical hyphenated text form; the FFI boundary carries it as the
// tagged object {"$uuid": "<text>"}.
type UUID string

// prepareValue rewrites parameter values so types JSON cannot express
// reach the FFI boundary intact: []byte becomes {"$binary": "<base64>"}
// and UUID becomes {"$uuid": "<text>"}. It recurses into lists and maps.
func prepareValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return map[string]any{"$binary": base64.StdEncoding.EncodeToString(v)}
	case UUID:
		return map[string]any{"$uuid": string(v)}
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = prepareValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = prepareValue(item)
		}
		return out
	default:
		return value
	}
}

// normalizeValue turns json.Number into int64 or float64, decodes the
// {"$binary": "<base64>"} tagged object back into []byte, and recurses
// into lists and maps so callers see plain Go values.
func normalizeValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		f, _ := v.Float64()
		return f
	case []any:
		for i, item := range v {
			v[i] = normalizeValue(item)
		}
		return v
	case map[string]any:
		if len(v) == 1 {
			if encoded, ok := v["$binary"].(string); ok {
				if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
					return decoded
				}
			}
			if encoded, ok := v["$uuid"].(string); ok {
				return UUID(encoded)
			}
		}
		for key, item := range v {
			v[key] = normalizeValue(item)
		}
		return v
	default:
		return value
	}
}

// Close releases the database handle. It is safe to call more than once.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	db.lib.close(db.ptr)
	db.ptr = nil
	return nil
}

// Version returns the HawDB version string reported by this library.
func (lib *Library) Version() string {
	var out ffiBuffer
	lib.version(&out)
	return string(lib.takeBytes(&out))
}

// Version returns the HawDB version string of the default library, or an
// empty string if it cannot be loaded.
func Version() string {
	lib, err := defaultLibrary()
	if err != nil {
		return ""
	}
	return lib.Version()
}
