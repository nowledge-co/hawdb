// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//go:build hawdb_boundary_profile

package hawdb

import (
	"math"
	"sync/atomic"
	"time"
)

const boundaryProfiling = true

// BoundaryProfile contains cumulative host-side wall times for developer
// qualification builds using hawdb_boundary_profile. Parameter conversion
// includes prepareValue and JSON encoding; result conversion includes native
// buffer copying/freeing, JSON decoding and normalization (or error decoding).
// FFICallNS includes the entire native call; native profile phases nest inside
// it and must not be added to it. Lock acquisition and result consumption are
// outside these phases. Snapshots read counters independently; use serial
// queries and before/after differences for operation-level measurements.
type BoundaryProfile struct {
	ParameterConversionNS    uint64 `json:"parameter_conversion_ns"`
	ParameterConversionCalls uint64 `json:"parameter_conversion_calls"`
	FFICallNS                uint64 `json:"ffi_call_ns"`
	FFICalls                 uint64 `json:"ffi_calls"`
	ResultConversionNS       uint64 `json:"result_conversion_ns"`
	ResultConversionCalls    uint64 `json:"result_conversion_calls"`
	Saturated                bool   `json:"saturated"`
}

type boundaryProfileState struct {
	parameterNS    atomic.Uint64
	parameterCalls atomic.Uint64
	ffiNS          atomic.Uint64
	ffiCalls       atomic.Uint64
	resultNS       atomic.Uint64
	resultCalls    atomic.Uint64
	saturated      atomic.Bool
}

func (p *boundaryProfileState) add(counter *atomic.Uint64, value uint64) {
	for {
		before := counter.Load()
		after := before + value
		if value > math.MaxUint64-before {
			after = math.MaxUint64
			p.saturated.Store(true)
		}
		if counter.CompareAndSwap(before, after) {
			return
		}
	}
}

func (p *boundaryProfileState) record(ns, calls *atomic.Uint64, started time.Time) {
	p.add(ns, uint64(max(time.Since(started).Nanoseconds(), 0)))
	p.add(calls, 1)
}

func (p *boundaryProfileState) recordParameter(started time.Time) {
	p.record(&p.parameterNS, &p.parameterCalls, started)
}

func (p *boundaryProfileState) recordFFI(started time.Time) {
	p.record(&p.ffiNS, &p.ffiCalls, started)
}

func (p *boundaryProfileState) recordResult(started time.Time) {
	p.record(&p.resultNS, &p.resultCalls, started)
}

// BoundaryProfile returns host-side counters, including failed query phases.
// It remains readable after Close. Saturated profiles cannot qualify timing.
func (db *DB) BoundaryProfile() BoundaryProfile {
	p := &db.boundaryProfile
	return BoundaryProfile{
		ParameterConversionNS: p.parameterNS.Load(), ParameterConversionCalls: p.parameterCalls.Load(),
		FFICallNS: p.ffiNS.Load(), FFICalls: p.ffiCalls.Load(),
		ResultConversionNS: p.resultNS.Load(), ResultConversionCalls: p.resultCalls.Load(),
		Saturated: p.saturated.Load(),
	}
}
