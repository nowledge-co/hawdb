// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//go:build hawdb_boundary_profile

package hawdb

import (
	"bytes"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// A Go callback stands in for the C ABI so conversion, errors and ownership can
// be checked independently of database execution or an installed native library.
func profileFixture(wire string, success bool) (*DB, *atomic.Uint64) {
	data := []byte(wire)
	freed := new(atomic.Uint64)
	call := func(_ unsafe.Pointer, _ *byte, _ uintptr, _ *byte, _ uintptr, result, failure *ffiBuffer) bool {
		target := result
		if !success {
			target = failure
		}
		*target = ffiBuffer{ptr: unsafe.SliceData(data), len: uintptr(len(data))}
		return success
	}
	return &DB{lib: &Library{query: call, querySQL: call,
		bufferFree: func(buffer *ffiBuffer) { freed.Add(1); *buffer = ffiBuffer{} },
		close:      func(unsafe.Pointer) {},
	}}, freed
}

func TestBoundaryProfileConversionsAndBufferRelease(t *testing.T) {
	db, freed := profileFixture(`{"columns":["value"],"rows":[[{"$binary":"AAE="}]]}`, true)
	original := db.lib.query
	db.lib.query = func(ptr unsafe.Pointer, text *byte, textLen uintptr, params *byte, paramsLen uintptr, result, failure *ffiBuffer) bool {
		encoded := unsafe.Slice(params, paramsLen)
		if !bytes.Contains(encoded, []byte(`"$binary":"AAE="`)) {
			t.Errorf("tagged parameter missing: %s", encoded)
		}
		time.Sleep(2 * time.Millisecond)
		return original(ptr, text, textLen, params, paramsLen, result, failure)
	}
	result, err := db.Query("RETURN $value", map[string]any{"value": []byte{0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Rows[0][0].([]byte), []byte{0, 1}) {
		t.Fatal(result)
	}
	p := db.BoundaryProfile()
	if p.ParameterConversionCalls != 1 || p.FFICalls != 1 || p.ResultConversionCalls != 1 || p.Saturated {
		t.Fatal(p)
	}
	if p.FFICallNS < uint64(2*time.Millisecond) {
		t.Fatalf("native call interval was omitted: %+v", p)
	}
	if freed.Load() != 1 {
		t.Fatalf("native result not released: %d", freed.Load())
	}
	db.Close()
	if db.BoundaryProfile() != p {
		t.Fatal("close changed query counters")
	}
	if _, err := db.Query("closed", nil); err == nil {
		t.Fatal("closed handle accepted")
	}
	if db.BoundaryProfile() != p {
		t.Fatal("closed rejection counted conversion work")
	}
}

func TestBoundaryProfileParameterFailureDoesNotCountNativeWork(t *testing.T) {
	db, _ := profileFixture(`{"columns":[],"rows":[]}`, true)
	_, err := db.Query("RETURN $bad", map[string]any{"bad": make(chan int)})
	if err == nil || !strings.Contains(err.Error(), "marshal parameters") {
		t.Fatal(err)
	}
	p := db.BoundaryProfile()
	if p.ParameterConversionCalls != 1 || p.FFICalls != 0 || p.ResultConversionCalls != 0 {
		t.Fatal(p)
	}
}

func TestBoundaryProfileErrorAndMalformedOutput(t *testing.T) {
	for _, tc := range []struct {
		wire      string
		success   bool
		errorText string
	}{
		{`{"kind":"admission","message":"refused"}`, false, "admission: refused"},
		{`{`, true, "malformed result JSON"},
	} {
		db, freed := profileFixture(tc.wire, tc.success)
		_, err := db.QuerySQL("SELECT 1")
		if err == nil || !strings.Contains(err.Error(), tc.errorText) {
			t.Fatalf("%q: %v", tc.wire, err)
		}
		p := db.BoundaryProfile()
		if p.ParameterConversionCalls != 1 || p.FFICalls != 1 || p.ResultConversionCalls != 1 || p.Saturated {
			t.Fatal(p)
		}
		if freed.Load() != 1 {
			t.Fatal("failure buffer was not released")
		}
	}
}

func TestBoundaryProfileConcurrentQueries(t *testing.T) {
	db, freed := profileFixture(`{"columns":["value"],"rows":[[1]]}`, true)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				if _, err := db.Query("RETURN 1", nil); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	p := db.BoundaryProfile()
	if p.ParameterConversionCalls != 80 || p.FFICalls != 80 || p.ResultConversionCalls != 80 || freed.Load() != 80 {
		t.Fatal(p, freed.Load())
	}
}

func TestBoundaryProfileOverflowSaturates(t *testing.T) {
	db := new(DB)
	p := &db.boundaryProfile
	p.parameterNS.Store(math.MaxUint64 - 1)
	p.add(&p.parameterNS, 2)
	p.add(&p.parameterNS, 10)
	snapshot := db.BoundaryProfile()
	if snapshot.ParameterConversionNS != math.MaxUint64 || !snapshot.Saturated {
		t.Fatal(snapshot)
	}
}
