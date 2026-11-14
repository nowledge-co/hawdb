// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//go:build !hawdb_boundary_profile

package main

import hawdb "github.com/nowledge-co/hawdb/bindings/go"

const hostProfiling = false

func hostProfileReader(*hawdb.DB) func() hostSnapshot { return nil }
