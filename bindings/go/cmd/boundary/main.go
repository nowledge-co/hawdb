// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

// Local benchmark wrapper around the ordinary purego binding.
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"time"

	"github.com/ebitengine/purego"
	hawdb "github.com/nowledge-co/hawdb/bindings/go"
)

type job struct {
	Case         string           `json:"case"`
	Backend      string           `json:"backend"`
	Path         string           `json:"path"`
	Rows         []map[string]any `json:"rows"`
	InsertSingle string           `json:"insert_single"`
	InsertBulk   string           `json:"insert_bulk"`
	Scan         string           `json:"scan"`
	Point        string           `json:"point"`
	Columns      []string         `json:"columns"`
	CPUProfile   string           `json:"cpu_profile_path"`
}

type nativeSnapshot struct {
	AllocationCalls           uint64
	AllocatedBytes            uint64
	DeallocationCalls         uint64
	DeallocatedBytes          uint64
	ReallocationCalls         uint64
	LiveRequestedBytes        uint64
	ProcessPeakRequestedBytes uint64
	ParameterConversionNS     uint64
	EngineCallNS              uint64
	ResultConversionNS        uint64
}

func profileReader(path string) func(*nativeSnapshot) {
	handle, err := loadProfileLibrary(path)
	if err != nil {
		return nil
	}
	symbol, err := lookupProfileSymbol(handle, "hawdb_boundary_profile_snapshot")
	if err != nil {
		return nil
	}
	var read func(*nativeSnapshot)
	purego.RegisterFunc(&read, symbol)
	return read
}

func profileDelta(before, after nativeSnapshot) map[string]any {
	return map[string]any{
		"allocation_calls":             after.AllocationCalls - before.AllocationCalls,
		"allocated_bytes":              after.AllocatedBytes - before.AllocatedBytes,
		"deallocation_calls":           after.DeallocationCalls - before.DeallocationCalls,
		"deallocated_bytes":            after.DeallocatedBytes - before.DeallocatedBytes,
		"reallocation_calls":           after.ReallocationCalls - before.ReallocationCalls,
		"live_requested_bytes_before":  before.LiveRequestedBytes,
		"live_requested_bytes_after":   after.LiveRequestedBytes,
		"process_peak_requested_bytes": after.ProcessPeakRequestedBytes,
		"parameter_conversion_ns":      after.ParameterConversionNS - before.ParameterConversionNS,
		"engine_call_ns":               after.EngineCallNS - before.EngineCallNS,
		"result_conversion_ns":         after.ResultConversionNS - before.ResultConversionNS,
		"scope":                        "Rust allocator requests; Go heap/rounding/non-Rust workspace excluded",
	}
}

func input(value any) any {
	switch value := value.(type) {
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer
		}
		float, err := value.Float64()
		if err != nil {
			panic(err)
		}
		return float
	case []any:
		for i := range value {
			value[i] = input(value[i])
		}
		return value
	case map[string]any:
		for key := range value {
			value[key] = input(value[key])
		}
		return value
	default:
		return value
	}
}

type checksum uint64

func (c *checksum) bytes(data []byte) {
	for _, b := range data {
		*c = checksum((uint64(*c) ^ uint64(b)) * 0x100000001b3)
	}
}
func (c *checksum) length(n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	c.bytes(b[:])
}
func (c *checksum) value(value any) {
	switch value := value.(type) {
	case nil:
		c.bytes([]byte{0})
	case bool:
		b := byte(0)
		if value {
			b = 1
		}
		c.bytes([]byte{1, b})
	case int64:
		c.bytes([]byte{2})
		c.length(uint64(value))
	case float64:
		c.bytes([]byte{3})
		c.length(math.Float64bits(value))
	case string:
		c.bytes([]byte{4})
		c.length(uint64(len(value)))
		c.bytes([]byte(value))
	case []byte:
		c.bytes([]byte{5})
		c.length(uint64(len(value)))
		c.bytes(value)
	case []any:
		c.bytes([]byte{7})
		c.length(uint64(len(value)))
		for _, item := range value {
			c.value(item)
		}
	case map[string]any:
		c.bytes([]byte{8})
		c.length(uint64(len(value)))
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			c.value(key)
			c.value(value[key])
		}
	default:
		panic(fmt.Sprintf("unexpected result type %T", value))
	}
}

func run(j job, lib *hawdb.Library, nativeRead func(*nativeSnapshot)) (map[string]any, error) {
	var db *hawdb.DB
	var err error
	if j.Backend == "memory" {
		db, err = lib.OpenInMemory()
	} else if j.Backend == "file" {
		db, err = lib.Open(j.Path)
	} else {
		return nil, fmt.Errorf("unknown backend")
	}
	if err != nil {
		return nil, err
	}
	defer db.Close()
	setup := time.Now()
	if err := db.Exec("CREATE INDEX ON :Boundary(id)", nil); err != nil {
		return nil, fmt.Errorf("setup: %w", err)
	}
	if j.Case == "select" || j.Case == "point" || j.Case == "wide" {
		for start := 0; start < len(j.Rows); start += 512 {
			end := start + 512
			if end > len(j.Rows) {
				end = len(j.Rows)
			}
			if err := db.Exec(j.InsertBulk, map[string]any{"rows": j.Rows[start:end]}); err != nil {
				return nil, fmt.Errorf("setup: %w", err)
			}
		}
	}
	setupNS := time.Since(setup).Nanoseconds()
	if j.Case == "select" || j.Case == "point" || j.Case == "wide" {
		calls := 1
		if j.Case == "point" {
			calls = len(j.Rows)
		}
		for i := 0; i < calls; i++ {
			text := j.Scan
			var params map[string]any
			if j.Case == "point" {
				text = j.Point
				params = map[string]any{"id": j.Rows[i]["id"]}
			}
			if _, err := db.Query(text, params); err != nil {
				return nil, fmt.Errorf("warmup: %w", err)
			}
		}
	}
	var before, after runtime.MemStats
	if j.CPUProfile != "" {
		file, err := os.Create(j.CPUProfile)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		if err := pprof.StartCPUProfile(file); err != nil {
			return nil, err
		}
		defer pprof.StopCPUProfile()
	}
	runtime.ReadMemStats(&before)
	c := checksum(0xcbf29ce484222325)
	c.bytes([]byte("HDBBOUND1"))
	c.length(uint64(len(j.Columns)))
	for _, column := range j.Columns {
		c.value(column)
	}
	var nativeBefore, nativeAfter nativeSnapshot
	if nativeRead != nil {
		nativeRead(&nativeBefore)
	}
	var queryNS, consumeNS int64
	rowsSeen := 0
	started := time.Now()
	if j.Case == "fill" {
		for _, row := range j.Rows {
			called := time.Now()
			err := db.Exec(j.InsertSingle, row)
			queryNS += time.Since(called).Nanoseconds()
			if err != nil {
				return nil, fmt.Errorf("execute: %w", err)
			}
		}
	} else if j.Case == "fill_bulk" {
		called := time.Now()
		err := db.Exec(j.InsertBulk, map[string]any{"rows": j.Rows})
		queryNS += time.Since(called).Nanoseconds()
		if err != nil {
			return nil, fmt.Errorf("execute: %w", err)
		}
	}
	writeNS := queryNS
	calls := 1
	if j.Case == "point" {
		calls = len(j.Rows)
	}
	for i := 0; i < calls; i++ {
		text := j.Scan
		var params map[string]any
		if j.Case == "point" {
			text = j.Point
			params = map[string]any{"id": j.Rows[i]["id"]}
		}
		called := time.Now()
		output, err := db.Query(text, params)
		queryNS += time.Since(called).Nanoseconds()
		if err != nil {
			return nil, fmt.Errorf("execute: %w", err)
		}
		if len(output.Columns) != len(j.Columns) {
			return nil, fmt.Errorf("consumer: schema mismatch")
		}
		for k := range j.Columns {
			if output.Columns[k] != j.Columns[k] {
				return nil, fmt.Errorf("consumer: schema mismatch")
			}
		}
		consumed := time.Now()
		for _, values := range output.Rows {
			c.bytes([]byte{0xff})
			c.length(uint64(len(values)))
			for _, value := range values {
				c.value(value)
			}
			rowsSeen++
		}
		consumeNS += time.Since(consumed).Nanoseconds()
	}
	elapsedNS := time.Since(started).Nanoseconds()
	var profile map[string]any
	if nativeRead != nil {
		nativeRead(&nativeAfter)
		profile = profileDelta(nativeBefore, nativeAfter)
	}
	runtime.ReadMemStats(&after)
	return map[string]any{"status": "ok", "layer": "go", "case": j.Case, "backend": j.Backend, "input_rows": len(j.Rows), "output_rows": rowsSeen, "values": rowsSeen * len(j.Columns), "checksum": fmt.Sprintf("%016x", uint64(c)), "setup_ns": setupNS, "query_boundary_ns": queryNS, "write_boundary_ns": writeNS, "read_boundary_ns": queryNS - writeNS, "consumer_ns": consumeNS, "elapsed_ns": elapsedNS, "go_allocations": after.Mallocs - before.Mallocs, "go_allocated_bytes": after.TotalAlloc - before.TotalAlloc, "native_profile": profile, "cpu_profile": j.CPUProfile, "durability": "SyncOnEveryWrite", "cgo_enabled": false}, nil
}

func main() {
	if len(os.Args) != 3 {
		panic("Usage: boundary JOB.json LIBHAWDB_FFI")
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	var j job
	if err := decoder.Decode(&j); err != nil {
		panic(err)
	}
	for _, row := range j.Rows {
		input(row)
	}
	lib, err := hawdb.LoadLibrary(os.Args[2])
	var result map[string]any
	if err == nil {
		result, err = run(j, lib, profileReader(os.Args[2]))
	}
	if err != nil {
		result = map[string]any{"status": "error", "layer": "go", "error": err.Error()}
	}
	result["go_version"] = runtime.Version()
	result["pointer_bits"] = strconv.IntSize
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}
