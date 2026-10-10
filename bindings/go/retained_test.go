// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

package hawdb

import (
	"errors"
	"io"
	"math"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

const retainedQuery = "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score, id(n) AS identity, n.score AS again"

func retainedFixture(t testing.TB) *DB {
	t.Helper()
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, statement := range []string{
		"CREATE NODE TABLE Item",
		"CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT",
	} {
		if err := db.Exec(statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	for score := int64(0); score < 9; score++ {
		if err := db.Exec("CREATE (:Item {score: $score})", map[string]any{"score": score}); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func retainedCursor(t testing.TB, db *DB, query string, rows uint32) *RetainedCursor {
	t.Helper()
	cursor, err := db.QueryRetained(query, map[string]any{"min": int64(0)}, RetainedOptions{BatchRows: rows})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cursor.Close() })
	return cursor
}

func retainedBatch(t testing.TB, cursor *RetainedCursor) *RetainedBatch {
	t.Helper()
	batch, err := cursor.Next()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { batch.Close() })
	return batch
}

func retainedColumn(t *testing.T, batch *RetainedBatch, index uint64) *RetainedColumn {
	t.Helper()
	column, err := batch.Column(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { column.Close() })
	return column
}

func assertRetainedCode(t *testing.T, err error, code RetainedCode) {
	t.Helper()
	var typed *RetainedError
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("expected retained code %d, got %v", code, err)
	}
}

func TestRetainedABIWidths(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("retained ABI adapter is 64-bit only")
	}
	for _, check := range []struct {
		name             string
		actual, expected uintptr
	}{
		{"request", unsafe.Sizeof(retainedRequest{}), 64},
		{"buffer", unsafe.Sizeof(retainedBuffer{}), 64},
		{"cursor", unsafe.Sizeof(retainedCursorDescriptor{}), 32},
		{"batch", unsafe.Sizeof(retainedBatchDescriptor{}), 112},
		{"schema", unsafe.Sizeof(retainedSchemaDescriptor{}), 40},
		{"column", unsafe.Sizeof(retainedColumnDescriptor{}), 280},
		{"state", unsafe.Sizeof(retainedStateDescriptor{}), 56},
		{"request text", unsafe.Offsetof(retainedRequest{}.text), 8},
		{"column values", unsafe.Offsetof(retainedColumnDescriptor{}.values), 80},
		{"batch selection", unsafe.Offsetof(retainedBatchDescriptor{}.selection), 48},
	} {
		if check.actual != check.expected {
			t.Errorf("%s: got %d want %d", check.name, check.actual, check.expected)
		}
	}
}

func TestRetainedIdentitySelectionAndIndependentLifetime(t *testing.T) {
	db := retainedFixture(t)
	cursor := retainedCursor(t, db, retainedQuery+" SKIP 1 LIMIT 2", 9)
	schema, err := cursor.SchemaCopy(0)
	if err != nil || schema.Name != "score" || schema.Type != RetainedInt64 || !schema.Nullable {
		t.Fatalf("schema=%+v err=%v", schema, err)
	}
	batch := retainedBatch(t, cursor)
	if rows, err := batch.Rows(); err != nil || rows != 2 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	column := retainedColumn(t, batch, 0)
	again := retainedColumn(t, batch, 2)
	identity := retainedColumn(t, batch, 1)
	retained, err := column.Retain()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { retained.Close() })
	info, _ := column.BufferInfo()
	againInfo, _ := again.BufferInfo()
	retainedInfo, _ := retained.BufferInfo()
	if info != againInfo || info != retainedInfo || column.descriptor.values.data != again.descriptor.values.data || info.Allocation == 0 {
		t.Fatal("projection or retain changed allocation identity/range")
	}
	if column.descriptor.selection.offset != 4 {
		t.Fatal("SKIP selection was gathered or its offset lost")
	}
	for i := uint64(0); i < 2; i++ {
		value, valid, err := column.Int64At(i)
		if err != nil || !valid || value != int64(i+1) {
			t.Fatalf("row %d: %d %v %v", i, value, valid, err)
		}
		if _, valid, err := identity.UInt64At(i); err != nil || !valid {
			t.Fatalf("identity: %v %v", valid, err)
		}
	}
	_, _, err = column.Float64At(0)
	assertRetainedCode(t, err, RetainedUnsupportedType)
	_, _, err = column.Int64At(2)
	assertRetainedCode(t, err, RetainedInvalidArgument)
	if next, err := cursor.Next(); next != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("next=%v err=%v", next, err)
	}
	if state, err := column.State(); err != nil || state.Status != 1 {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	runtime.GC()
	if value, valid, err := column.Int64At(0); err != nil || !valid || value != 1 {
		t.Fatalf("closed parent revoked column: %d %v %v", value, valid, err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, err = column.Int64At(0)
	assertRetainedCode(t, err, RetainedClosed)
	if value, valid, err := retained.Int64At(1); err != nil || !valid || value != 2 {
		t.Fatalf("independent retain: %d %v %v", value, valid, err)
	}
	_, err = cursor.Next()
	assertRetainedCode(t, err, RetainedClosed)
	_, err = batch.Rows()
	assertRetainedCode(t, err, RetainedClosed)
}

func TestRetainedBackpressureDoesNotAdvanceAndExplicitReleaseResumes(t *testing.T) {
	db := retainedFixture(t)
	cursor := retainedCursor(t, db, retainedQuery, 3)
	first := retainedBatch(t, cursor)
	firstColumn := retainedColumn(t, first, 0)
	second := retainedBatch(t, cursor)
	secondColumn := retainedColumn(t, second, 0)
	first.Close()
	second.Close()
	before, err := cursor.State()
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := cursor.Next(); batch != nil {
		t.Fatal("pressure returned a batch")
	} else {
		assertRetainedCode(t, err, RetainedBackpressure)
	}
	after, err := cursor.State()
	if err != nil || after.VisitedRows != before.VisitedRows || before.VisitedRows != 6 {
		t.Fatalf("pressure advanced source: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := firstColumn.Close(); err != nil {
		t.Fatal(err)
	}
	third := retainedBatch(t, cursor)
	thirdColumn := retainedColumn(t, third, 0)
	if value, valid, err := thirdColumn.Int64At(0); err != nil || !valid || value != 6 {
		t.Fatalf("retry position: %d %v %v", value, valid, err)
	}
	if value, valid, err := secondColumn.Int64At(0); err != nil || !valid || value != 3 {
		t.Fatalf("older held column: %d %v %v", value, valid, err)
	}
}

func TestRetainedEmptySchemaRefusalsAndOldLibrary(t *testing.T) {
	db := retainedFixture(t)
	cursor, err := db.QueryRetained(retainedQuery, map[string]any{"min": 100}, RetainedOptions{BatchRows: 9})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	batch := retainedBatch(t, cursor)
	if rows, err := batch.Rows(); err != nil || rows != 0 {
		t.Fatalf("empty batch rows=%d err=%v", rows, err)
	}
	if next, err := cursor.Next(); next != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("next=%v err=%v", next, err)
	}
	if schema, err := cursor.SchemaCopy(1); err != nil || schema.Type != RetainedUInt64 || !schema.NodeIdentity || schema.Nullable {
		t.Fatalf("empty schema=%+v err=%v", schema, err)
	}
	for _, options := range []RetainedOptions{{Writable: true}, {RequireSourceReuse: true}} {
		if cursor, err := db.QueryRetained(retainedQuery, map[string]any{"min": 0}, options); cursor != nil {
			t.Fatal("copy request returned cursor")
		} else {
			assertRetainedCode(t, err, RetainedCopyRequired)
		}
	}
	if cursor, err := db.QueryRetained("MATCH (n:Item) RETURN count(n)", nil, RetainedOptions{}); cursor != nil {
		t.Fatal("unsupported plan returned cursor")
	} else {
		assertRetainedCode(t, err, RetainedUnsupportedPlan)
	}
	old := &DB{lib: &Library{}}
	if _, err := old.QueryRetained(retainedQuery, nil, RetainedOptions{}); err == nil {
		t.Fatal("old library accepted retained query")
	} else {
		var typed *Error
		if !errors.As(err, &typed) || typed.Kind != "capability_unavailable" {
			t.Fatal(err)
		}
	}
}

func TestRetainedReadAndCloseAreSerialized(t *testing.T) {
	db := retainedFixture(t)
	cursor := retainedCursor(t, db, retainedQuery, 9)
	column := retainedColumn(t, retainedBatch(t, cursor), 0)
	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		defer close(done)
		value, valid, err := column.Int64At(8)
		if err != nil || !valid || value != 8 {
			t.Errorf("first concurrent read: %d %v %v", value, valid, err)
		}
		close(ready)
		for i := 0; i < 1000; i++ {
			value, valid, err := column.Int64At(8)
			if err != nil {
				var typed *RetainedError
				if !errors.As(err, &typed) || typed.Code != RetainedClosed {
					t.Errorf("concurrent close returned %v", err)
				}
				return
			}
			if !valid || value != 8 {
				t.Errorf("concurrent read: %d %v", value, valid)
				return
			}
		}
	}()
	<-ready
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	_, _, err := column.Int64At(8)
	assertRetainedCode(t, err, RetainedClosed)
}

func TestRetainedBatchBorrowsOncePerColumnAcrossCopiedOwners(t *testing.T) {
	values := []int64{7, 11}
	selection := []uint32{0, 1}
	borrowCalls := 0
	lib := &Library{retained: &retainedFunctions{
		columnBorrow: func(namespace, id, index uint64, out *retainedColumnDescriptor, size uint32) uint32 {
			borrowCalls++
			*out = retainedColumnDescriptor{
				version: 1, physical: 2, selected: 2,
				schema:       retainedSchemaDescriptor{dataType: uint32(RetainedInt64)},
				values:       retainedBuffer{data: unsafe.Pointer(&values[0]), length: 16},
				selection:    retainedBuffer{data: unsafe.Pointer(&selection[0]), length: 8},
				validityKind: 1,
			}
			return uint32(RetainedOK)
		},
		release: func(uint64, uint64) uint32 { return uint32(RetainedOK) },
	}}
	batch := &RetainedBatch{owner: newRetainedOwner(lib, 1, 1), descriptor: retainedBatchDescriptor{columns: 1}, columns: &retainedBatchColumns{}}
	copy := *batch
	for i := 0; i < 100; i++ {
		for _, view := range []*RetainedBatch{batch, &copy} {
			value, valid, err := view.Int64At(0, 1)
			if err != nil || !valid || value != 11 {
				t.Fatalf("cached read: %d %v %v", value, valid, err)
			}
		}
	}
	if borrowCalls != 1 {
		t.Fatalf("borrow calls=%d, want 1", borrowCalls)
	}
	if err := copy.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, err := batch.Int64At(0, 0)
	assertRetainedCode(t, err, RetainedClosed)
	runtime.KeepAlive(values)
	runtime.KeepAlive(selection)
}

func TestRetainedCloseFailureAllowsExplicitRetry(t *testing.T) {
	calls := 0
	lib := &Library{retained: &retainedFunctions{release: func(uint64, uint64) uint32 {
		calls++
		if calls == 1 {
			return uint32(RetainedPanic)
		}
		return uint32(RetainedOK)
	}}}
	owner := newRetainedOwner(lib, 1, 1)
	assertRetainedCode(t, owner.close(), RetainedPanic)
	if owner.closed {
		t.Fatal("failed release discarded the owner")
	}
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	if !owner.closed || calls != 2 {
		t.Fatalf("closed=%v calls=%d", owner.closed, calls)
	}
}

func BenchmarkRetainedBatchScalarReads(b *testing.B) {
	db := retainedFixture(b)
	cursor := retainedCursor(b, db, retainedQuery, 9)
	batch := retainedBatch(b, cursor)
	if _, _, err := batch.Int64At(0, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		position := uint64(i % 9)
		value, valid, err := batch.Int64At(0, position)
		if err != nil || !valid || value != int64(position) {
			b.Fatalf("read: %d %v %v", value, valid, err)
		}
	}
}

func TestRetainedSharedHandlePressureStillAllowsBorrowedBatchReads(t *testing.T) {
	db := retainedFixture(t)
	cursor := retainedCursor(t, db, retainedQuery, 9)
	batch := retainedBatch(t, cursor)
	column := retainedColumn(t, batch, 0)
	var held []*RetainedColumn
	defer func() {
		for _, view := range held {
			view.Close()
		}
	}()
	for {
		view, err := column.Retain()
		if err != nil {
			assertRetainedCode(t, err, RetainedBackpressure)
			break
		}
		held = append(held, view)
		if len(held) > 1024 {
			t.Fatal("default aggregate handle allowance was bypassed")
		}
	}
	if len(held) != 1021 {
		t.Fatalf("unexpected independently admitted handles: %d", len(held))
	}
	if view, err := batch.Column(2); view != nil {
		t.Fatal("exhaustion returned a view")
	} else {
		assertRetainedCode(t, err, RetainedBackpressure)
	}
	if value, valid, err := batch.Int64At(0, 8); err != nil || !valid || value != 8 {
		t.Fatalf("borrowed consumption under pressure: %d %v %v", value, valid, err)
	}
	if next, err := db.QueryRetained(retainedQuery, map[string]any{"min": 0}, RetainedOptions{}); next != nil {
		next.Close()
		t.Fatal("second cursor multiplied the shared allowance")
	} else {
		assertRetainedCode(t, err, RetainedBackpressure)
	}
	if err := held[0].Close(); err != nil {
		t.Fatal(err)
	}
	retry, err := batch.Column(2)
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Close()
	if value, valid, err := retry.Int64At(8); err != nil || !valid || value != 8 {
		t.Fatalf("handle retry: %d %v %v", value, valid, err)
	}
}

func TestRetainedFloatBitsAndNullablePhysicalLayout(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		"CREATE NODE TABLE FloatItem",
		"CREATE PROPERTY ON NODE TABLE FloatItem(score) TYPE FLOAT",
		"CREATE (:FloatItem {score: -0.0})",
		"CREATE (:FloatItem {score: 0.0})",
		"CREATE (:FloatItem {score: null})",
		"CREATE (:FloatItem {score: 1.5})",
	} {
		if err := db.Exec(statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	cursor, err := db.QueryRetained("MATCH (n:FloatItem) WHERE n.score >= $min RETURN n.score AS score", map[string]any{"min": -1.0}, RetainedOptions{BatchRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	batch := retainedBatch(t, cursor)
	column := retainedColumn(t, batch, 0)
	info, err := column.Info()
	if err != nil || info.PhysicalRows != 4 || info.SelectedRows != 3 || info.Type != RetainedFloat64 || !info.Nullable {
		t.Fatalf("info=%+v err=%v", info, err)
	}
	for row, expected := range []uint64{1 << 63, 0, math.Float64bits(1.5)} {
		value, valid, err := column.Float64At(uint64(row))
		if err != nil || !valid || math.Float64bits(value) != expected {
			t.Fatalf("float row %d: bits=%x valid=%v err=%v", row, math.Float64bits(value), valid, err)
		}
		borrowed, valid, err := batch.Float64At(0, uint64(row))
		if err != nil || !valid || math.Float64bits(borrowed) != expected {
			t.Fatalf("borrowed float row %d", row)
		}
	}
	if column.descriptor.validityKind != 2 {
		t.Fatal("nullable physical validity was lost")
	}
	word := *(*uint64)(column.descriptor.validity.data)
	if word&15 != 11 {
		t.Fatalf("physical NULL bitmap: %b", word)
	}
	batch.Close()
	cursor.Close()
	db.Close()
	if value, valid, err := column.Float64At(0); err != nil || !valid || math.Float64bits(value) != 1<<63 {
		t.Fatalf("float after close: %x %v %v", math.Float64bits(value), valid, err)
	}
}

func TestRetainedFinalizerReleasesAbandonedColumn(t *testing.T) {
	db := retainedFixture(t)
	cursor := retainedCursor(t, db, retainedQuery, 3)
	first := retainedBatch(t, cursor)
	abandoned, err := first.Column(0)
	if err != nil {
		t.Fatal(err)
	}
	namespace, id := abandoned.owner.namespace, abandoned.owner.id
	first.Close()
	second := retainedBatch(t, cursor)
	_ = retainedColumn(t, second, 0)
	second.Close()
	if batch, err := cursor.Next(); batch != nil {
		t.Fatal("held owner did not consume a slot")
	} else {
		assertRetainedCode(t, err, RetainedBackpressure)
	}
	runtime.KeepAlive(abandoned)
	abandoned = nil
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		batch, err := cursor.Next()
		if err == nil {
			defer batch.Close()
			if value, valid, err := batch.Int64At(0, 0); err != nil || !valid || value != 6 {
				t.Fatalf("GC retry: %d %v %v", value, valid, err)
			}
			break
		}
		assertRetainedCode(t, err, RetainedBackpressure)
		if time.Now().After(deadline) {
			t.Fatal("abandoned column did not release through its finalizer")
		}
		time.Sleep(time.Millisecond)
	}
	var state retainedStateDescriptor
	code := db.lib.retained.state(namespace, id, &state, uint32(unsafe.Sizeof(state)))
	if RetainedCode(code) != RetainedInvalidHandle {
		t.Fatalf("finalizer left native handle live: %d", code)
	}
}

func TestRetainedIntegerBoundsAndAllNullLayout(t *testing.T) {
	for _, float := range []bool{false, true} {
		db, err := OpenInMemory()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		dataType := "INT"
		if float {
			dataType = "FLOAT"
		}
		if err := db.Exec("CREATE NODE TABLE Item", nil); err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("CREATE PROPERTY ON NODE TABLE Item(score) TYPE "+dataType, nil); err != nil {
			t.Fatal(err)
		}
		values := []any{int64(math.MinInt64), int64(math.MaxInt64)}
		var minimum any = int64(math.MinInt64)
		if float {
			values = []any{nil, nil}
			minimum = -1.0
		}
		for _, value := range values {
			if err := db.Exec("CREATE (:Item {score: $score})", map[string]any{"score": value}); err != nil {
				t.Fatal(err)
			}
		}
		cursor, err := db.QueryRetained(retainedQuery, map[string]any{"min": minimum}, RetainedOptions{BatchRows: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()
		batch := retainedBatch(t, cursor)
		column := retainedColumn(t, batch, 0)
		if float {
			info, err := column.Info()
			if err != nil || info.PhysicalRows != 2 || info.SelectedRows != 0 || info.Type != RetainedFloat64 || column.descriptor.validityKind != 2 || *(*uint64)(column.descriptor.validity.data) != 0 {
				t.Fatalf("all-null layout: info=%+v err=%v", info, err)
			}
		} else {
			for row, expected := range []int64{math.MinInt64, math.MaxInt64} {
				if value, valid, err := column.Int64At(uint64(row)); err != nil || !valid || value != expected {
					t.Fatalf("integer bounds: %d %v %v", value, valid, err)
				}
			}
		}
		if next, err := cursor.Next(); next != nil || !errors.Is(err, io.EOF) {
			t.Fatalf("completion: %v %v", next, err)
		}
	}
}
