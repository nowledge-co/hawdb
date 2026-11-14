// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//go:build hawdb_boundary_profile

package main

import hawdb "github.com/nowledge-co/hawdb/bindings/go"

const hostProfiling = true

func hostProfileReader(db *hawdb.DB) func() hostSnapshot {
	return func() hostSnapshot {
		p := db.BoundaryProfile()
		return hostSnapshot{
			values: [6]uint64{p.ParameterConversionNS, p.ParameterConversionCalls,
				p.FFICallNS, p.FFICalls, p.ResultConversionNS, p.ResultConversionCalls},
			saturated: p.Saturated,
		}
	}
}
