// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//go:build !hawdb_boundary_profile

package hawdb

import "time"

const boundaryProfiling = false

// Zero-sized in ordinary builds; constant-false guards remove all observers.
type boundaryProfileState struct{}

func (*boundaryProfileState) recordParameter(time.Time) {}
func (*boundaryProfileState) recordFFI(time.Time)       {}
func (*boundaryProfileState) recordResult(time.Time)    {}
