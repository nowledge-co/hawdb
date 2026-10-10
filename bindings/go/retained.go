// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

package hawdb

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// RetainedCode identifies an experimental native retained-interface outcome.
type RetainedCode uint32

const (
	RetainedOK RetainedCode = iota
	RetainedEOF
	RetainedBackpressure
	RetainedInvalidHandle
	RetainedInvalidArgument
	RetainedPanic
	RetainedClosed
	RetainedWorkingUnitTooLarge
	RetainedExecutionError
	RetainedResultBudget
	RetainedUnsupportedPlan
	RetainedUnsupportedLayout
	RetainedUnsupportedType
	RetainedCopyRequired
	RetainedSelectionRequiresMaterialization
	RetainedInvalidColumn
	RetainedSizeOverflow
	RetainedStopped
	RetainedAdmissionError
)

// RetainedError preserves the native outcome. Only Backpressure is retryable;
// release held batches/columns before retrying the same cursor.
type RetainedError struct{ Code RetainedCode }

func (e *RetainedError) Error() string {
	names := [...]string{"ok", "eof", "backpressure", "invalid_handle", "invalid_argument", "panic", "closed", "working_unit_too_large", "execution_error", "result_budget", "unsupported_plan", "unsupported_layout", "unsupported_type", "copy_required", "selection_requires_materialization", "invalid_column", "size_overflow", "stopped", "admission_error"}
	if uint32(e.Code) < uint32(len(names)) {
		return "hawdb: retained " + names[e.Code]
	}
	return fmt.Sprintf("hawdb: retained interface status %d", e.Code)
}

func (e *RetainedError) Retryable() bool { return e.Code == RetainedBackpressure }

func retainedError(code uint32) error {
	if RetainedCode(code) == RetainedOK {
		return nil
	}
	if RetainedCode(code) == RetainedEOF {
		return io.EOF
	}
	return &RetainedError{Code: RetainedCode(code)}
}

// RetainedOptions configures the experimental numeric cursor. Zero values use
// root defaults: 1,024 inspected rows, 1 MiB per batch, two payload slots.
// Larger requests still obey the database and shared runtime allowances.
type RetainedOptions struct {
	BatchRows          uint32
	OutstandingBatches uint32
	BatchBytes         uint64
	RequireSourceReuse bool
	Writable           bool
}

// These private descriptors mirror the fixed-width C ABI v1. Only 64-bit hosts
// are supported by this adapter; values never cross by-value struct arguments.
type retainedRequest struct {
	version, size   uint32
	text            *byte
	textLen         uint64
	params          *byte
	paramsLen       uint64
	rows, slots     uint32
	bytes           uint64
	flags, reserved uint32
}
type retainedBuffer struct {
	version, size                     uint32
	data                              unsafe.Pointer
	namespace, allocation, generation uint64
	capacity, offset, length          uint64
}
type retainedCursorDescriptor struct {
	version, size          uint32
	namespace, id, columns uint64
}
type retainedBatchDescriptor struct {
	version, size                              uint32
	namespace, id, physical, selected, columns uint64
	selection                                  retainedBuffer
}
type retainedSchemaDescriptor struct {
	version, size                   uint32
	name                            *byte
	nameLen                         uint64
	dataType, role, nullable, flags uint32
}
type retainedColumnDescriptor struct {
	version, size                     uint32
	namespace, id, physical, selected uint64
	schema                            retainedSchemaDescriptor
	values, selection, validity       retainedBuffer
	validityKind, flags               uint32
}
type retainedStateDescriptor struct {
	version, size, status, terminal                        uint32
	visited, emitted, constructed, pinnedRows, pinnedPages uint64
}
type retainedFunctions struct {
	query        func(unsafe.Pointer, *retainedRequest, uint32, *retainedCursorDescriptor, uint32) uint32
	next         func(uint64, uint64, *retainedBatchDescriptor, uint32) uint32
	schema       func(uint64, uint64, uint64, *retainedSchemaDescriptor, uint32) uint32
	column       func(uint64, uint64, uint64, *retainedColumnDescriptor, uint32) uint32
	columnBorrow func(uint64, uint64, uint64, *retainedColumnDescriptor, uint32) uint32
	batchRetain  func(uint64, uint64, *retainedBatchDescriptor, uint32) uint32
	columnRetain func(uint64, uint64, *retainedColumnDescriptor, uint32) uint32
	state        func(uint64, uint64, *retainedStateDescriptor, uint32) uint32
	release      func(uint64, uint64) uint32
}

func loadRetainedFunctions(handle uintptr) *retainedFunctions {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		return nil
	}
	names := [...]string{
		"hawdb_retained_abi_version", "hawdb_retained_query", "hawdb_retained_next",
		"hawdb_retained_schema", "hawdb_retained_column", "hawdb_retained_batch_retain",
		"hawdb_retained_column_retain", "hawdb_retained_state", "hawdb_retained_release",
		"hawdb_retained_column_borrow",
	}
	var symbols [len(names)]uintptr
	for i, name := range names {
		address, err := lookupSharedSymbol(handle, name)
		if err != nil {
			return nil
		}
		symbols[i] = address
	}
	var version func() uint32
	purego.RegisterFunc(&version, symbols[0])
	if version() != 1 {
		return nil
	}
	f := &retainedFunctions{}
	purego.RegisterFunc(&f.query, symbols[1])
	purego.RegisterFunc(&f.next, symbols[2])
	purego.RegisterFunc(&f.schema, symbols[3])
	purego.RegisterFunc(&f.column, symbols[4])
	purego.RegisterFunc(&f.batchRetain, symbols[5])
	purego.RegisterFunc(&f.columnRetain, symbols[6])
	purego.RegisterFunc(&f.state, symbols[7])
	purego.RegisterFunc(&f.release, symbols[8])
	purego.RegisterFunc(&f.columnBorrow, symbols[9])
	return f
}

// Every view keeps this independently releasable owner and its Library alive.
// Library mappings already live for the process lifetime. The lock covers both
// native calls and scalar pointer reads, so concurrent Close cannot revoke them.
type retainedOwner struct {
	mu            sync.RWMutex
	lib           *Library
	namespace, id uint64
	closed        bool
}

func newRetainedOwner(lib *Library, namespace, id uint64) *retainedOwner {
	owner := &retainedOwner{lib: lib, namespace: namespace, id: id}
	runtime.SetFinalizer(owner, (*retainedOwner).close)
	return owner
}
func (owner *retainedOwner) close() error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed {
		return nil
	}
	code := owner.lib.retained.release(owner.namespace, owner.id)
	if RetainedCode(code) == RetainedOK || RetainedCode(code) == RetainedInvalidHandle {
		owner.closed = true
		runtime.SetFinalizer(owner, nil)
	}
	return retainedError(code)
}
func (owner *retainedOwner) check() error {
	if owner.closed {
		return &RetainedError{Code: RetainedClosed}
	}
	return nil
}

// RetainedCursor pulls eligible numeric results without JSON result encoding.
// Results remain provisional until io.EOF; Close stops pulling and releases its
// source. Previously produced independently owned batches/columns stay readable.
type RetainedCursor struct {
	owner      *retainedOwner
	descriptor retainedCursorDescriptor
}

// QueryRetained creates an experimental numeric cursor. Unsupported plans,
// types, writable or source-reuse requests fail explicitly, with no fallback.
// Parameter JSON is input only. Complete source/workspace memory qualification
// remains unfinished; this API does not promise a whole-process RSS bound.
func (db *DB) QueryRetained(cypher string, params map[string]any, options RetainedOptions) (*RetainedCursor, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, &RetainedError{Code: RetainedClosed}
	}
	if db.lib.retained == nil {
		return nil, &Error{Kind: "capability_unavailable", Message: "retained numeric ABI v1 requires a compatible library and 64-bit host"}
	}
	var paramsJSON string
	if params != nil {
		data, err := json.Marshal(prepareValue(params))
		if err != nil {
			return nil, err
		}
		paramsJSON = string(data)
	}
	textPtr, textLen := strArg(cypher)
	paramsPtr, paramsLen := strArg(paramsJSON)
	request := retainedRequest{
		version: 1, size: uint32(unsafe.Sizeof(retainedRequest{})),
		text: textPtr, textLen: uint64(textLen), params: paramsPtr, paramsLen: uint64(paramsLen),
		rows: options.BatchRows, slots: options.OutstandingBatches, bytes: options.BatchBytes,
	}
	if options.RequireSourceReuse {
		request.flags |= 1
	}
	if options.Writable {
		request.flags |= 2
	}
	var descriptor retainedCursorDescriptor
	code := db.lib.retained.query(db.ptr, &request, request.size, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	runtime.KeepAlive(cypher)
	runtime.KeepAlive(paramsJSON)
	runtime.KeepAlive(db)
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return &RetainedCursor{owner: newRetainedOwner(db.lib, descriptor.namespace, descriptor.id), descriptor: descriptor}, nil
}

// Next returns a distinct payload lease, io.EOF, or an explicit failure.
// Backpressure does not advance the source; Close old views and retry.
func (cursor *RetainedCursor) Next() (*RetainedBatch, error) {
	owner := cursor.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedBatchDescriptor
	code := owner.lib.retained.next(owner.namespace, owner.id, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return &RetainedBatch{owner: newRetainedOwner(owner.lib, descriptor.namespace, descriptor.id), descriptor: descriptor, columns: &retainedBatchColumns{}}, nil
}

func (cursor *RetainedCursor) Close() error { return cursor.owner.close() }

func (cursor *RetainedCursor) ColumnCount() (uint64, error) {
	cursor.owner.mu.RLock()
	defer cursor.owner.mu.RUnlock()
	if err := cursor.owner.check(); err != nil {
		return 0, err
	}
	return cursor.descriptor.columns, nil
}

// RetainedSchema is an owned metadata copy, not a borrowed native name.
type RetainedSchema struct {
	Name         string
	Type         RetainedType
	NodeIdentity bool
	Nullable     bool
}

// RetainedType preserves the native fixed-width column type.
type RetainedType uint32

const (
	RetainedInt64   RetainedType = 1
	RetainedFloat64 RetainedType = 2
	RetainedUInt64  RetainedType = 3
)

// SchemaCopy explicitly copies small schema metadata, including before any
// pull and after empty completion. It never copies numeric result payload.
func (cursor *RetainedCursor) SchemaCopy(index uint64) (RetainedSchema, error) {
	owner := cursor.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return RetainedSchema{}, err
	}
	var descriptor retainedSchemaDescriptor
	code := owner.lib.retained.schema(owner.namespace, owner.id, index, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return RetainedSchema{}, err
	}
	if descriptor.nameLen > uint64(^uint(0)>>1) || (descriptor.nameLen != 0 && descriptor.name == nil) {
		return RetainedSchema{}, &RetainedError{Code: RetainedInvalidArgument}
	}
	name := string(unsafe.Slice(descriptor.name, int(descriptor.nameLen)))
	runtime.KeepAlive(owner)
	return RetainedSchema{Name: name, Type: RetainedType(descriptor.dataType), NodeIdentity: descriptor.role == 2, Nullable: descriptor.nullable != 0}, nil
}

// RetainedState is a current observation, not a cached completion claim.
// Status is 0 Open, 1 Completed, 2 Failed, 3 Closed. Produced values remain
// readable after late failure or cursor close while their own owner is live.
type RetainedState struct {
	Status                                                              uint32
	TerminalCode                                                        RetainedCode
	VisitedRows, EmittedRows, ConstructedBytes, PinnedRows, PinnedPages uint64
}

func (owner *retainedOwner) state() (RetainedState, error) {
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return RetainedState{}, err
	}
	var descriptor retainedStateDescriptor
	code := owner.lib.retained.state(owner.namespace, owner.id, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return RetainedState{}, err
	}
	return RetainedState{Status: descriptor.status, TerminalCode: RetainedCode(descriptor.terminal), VisitedRows: descriptor.visited, EmittedRows: descriptor.emitted, ConstructedBytes: descriptor.constructed, PinnedRows: descriptor.pinnedRows, PinnedPages: descriptor.pinnedPages}, nil
}

func (cursor *RetainedCursor) State() (RetainedState, error) { return cursor.owner.state() }

// RetainedBatch owns one immutable payload lease. Copying this Go value shares
// the same release state; Retain requests an independently admitted owner.
type RetainedBatch struct {
	owner      *retainedOwner
	descriptor retainedBatchDescriptor
	columns    *retainedBatchColumns
}
type retainedBatchColumns struct {
	mu          sync.Mutex
	descriptors []retainedColumnDescriptor
}

func (batch *RetainedBatch) Close() error                  { return batch.owner.close() }
func (batch *RetainedBatch) State() (RetainedState, error) { return batch.owner.state() }

func (batch *RetainedBatch) Rows() (uint64, error) {
	batch.owner.mu.RLock()
	defer batch.owner.mu.RUnlock()
	if err := batch.owner.check(); err != nil {
		return 0, err
	}
	return batch.descriptor.selected, nil
}

func (batch *RetainedBatch) Retain() (*RetainedBatch, error) {
	owner := batch.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedBatchDescriptor
	code := owner.lib.retained.batchRetain(owner.namespace, owner.id, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return &RetainedBatch{owner: newRetainedOwner(owner.lib, descriptor.namespace, descriptor.id), descriptor: descriptor, columns: &retainedBatchColumns{}}, nil
}

// Column creates an independent column owner. It survives batch, cursor and
// database closure. Close it explicitly to release capacity; GC is a safety net.
func (batch *RetainedBatch) Column(index uint64) (*RetainedColumn, error) {
	owner := batch.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedColumnDescriptor
	code := owner.lib.retained.column(owner.namespace, owner.id, index, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return &RetainedColumn{owner: newRetainedOwner(owner.lib, descriptor.namespace, descriptor.id), descriptor: descriptor}, nil
}

// borrowedColumn is used only under the batch owner lock. Its descriptor is
// prepaid by the native pull, and this temporary view creates no extra owner.
func (batch *RetainedBatch) borrowedColumn(index uint64) (RetainedColumn, error) {
	owner := batch.owner
	if err := owner.check(); err != nil {
		return RetainedColumn{}, err
	}
	if index >= batch.descriptor.columns {
		return RetainedColumn{}, &RetainedError{Code: RetainedInvalidColumn}
	}
	cache := batch.columns
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.descriptors == nil {
		// The native batch prepays one descriptor per column and wrapper padding.
		// Immutable pointers remain valid under the enclosing owner read lock.
		cache.descriptors = make([]retainedColumnDescriptor, batch.descriptor.columns)
	}
	descriptor := &cache.descriptors[index]
	if descriptor.version == 0 {
		var borrowed retainedColumnDescriptor
		code := owner.lib.retained.columnBorrow(owner.namespace, owner.id, index, &borrowed, uint32(unsafe.Sizeof(borrowed)))
		if err := retainedError(code); err != nil {
			return RetainedColumn{}, err
		}
		*descriptor = borrowed
	}
	return RetainedColumn{owner: owner, descriptor: *descriptor}, nil
}

// Int64At reads a selected scalar through the existing batch lease, requiring
// no independent column handle. Reading a held batch still works when further
// owner admission would return Backpressure. Close the batch after consuming it.
func (batch *RetainedBatch) Int64At(index, position uint64) (int64, bool, error) {
	batch.owner.mu.RLock()
	defer batch.owner.mu.RUnlock()
	column, err := batch.borrowedColumn(index)
	if err != nil {
		return 0, false, err
	}
	return column.readInt64(position)
}

func (batch *RetainedBatch) Float64At(index, position uint64) (float64, bool, error) {
	batch.owner.mu.RLock()
	defer batch.owner.mu.RUnlock()
	column, err := batch.borrowedColumn(index)
	if err != nil {
		return 0, false, err
	}
	return column.readFloat64(position)
}

func (batch *RetainedBatch) UInt64At(index, position uint64) (uint64, bool, error) {
	batch.owner.mu.RLock()
	defer batch.owner.mu.RUnlock()
	column, err := batch.borrowedColumn(index)
	if err != nil {
		return 0, false, err
	}
	return column.readUInt64(position)
}

// RetainedColumn is a read-only typed view. Getters read native memory directly
// under its release lock; no mutable Go slice or unleased pointer is exposed.
// Rows are selected result positions, preserving order without gathering.
type RetainedColumn struct {
	owner      *retainedOwner
	descriptor retainedColumnDescriptor
}

func (column *RetainedColumn) Close() error                  { return column.owner.close() }
func (column *RetainedColumn) State() (RetainedState, error) { return column.owner.state() }

// RetainedColumnInfo describes the physical allocation and selected result size.
type RetainedColumnInfo struct {
	PhysicalRows, SelectedRows uint64
	Type                       RetainedType
	NodeIdentity, Nullable     bool
}

func (column *RetainedColumn) Info() (RetainedColumnInfo, error) {
	column.owner.mu.RLock()
	defer column.owner.mu.RUnlock()
	if err := column.owner.check(); err != nil {
		return RetainedColumnInfo{}, err
	}
	d := &column.descriptor
	return RetainedColumnInfo{PhysicalRows: d.physical, SelectedRows: d.selected, Type: RetainedType(d.schema.dataType), NodeIdentity: d.schema.role == 2, Nullable: d.schema.nullable != 0}, nil
}

func (column *RetainedColumn) Retain() (*RetainedColumn, error) {
	owner := column.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedColumnDescriptor
	code := owner.lib.retained.columnRetain(owner.namespace, owner.id, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return &RetainedColumn{owner: newRetainedOwner(owner.lib, descriptor.namespace, descriptor.id), descriptor: descriptor}, nil
}

// RetainedBufferInfo reports identity and capacity without exposing a pointer.
type RetainedBufferInfo struct {
	Namespace, Allocation, Generation       uint64
	CapacityBytes, OffsetBytes, LengthBytes uint64
}

func (column *RetainedColumn) BufferInfo() (RetainedBufferInfo, error) {
	column.owner.mu.RLock()
	defer column.owner.mu.RUnlock()
	if err := column.owner.check(); err != nil {
		return RetainedBufferInfo{}, err
	}
	b := column.descriptor.values
	return RetainedBufferInfo{b.namespace, b.allocation, b.generation, b.capacity, b.offset, b.length}, nil
}

func (column *RetainedColumn) row(position uint64, expected RetainedType) (uint64, bool, error) {
	if err := column.owner.check(); err != nil {
		return 0, false, err
	}
	d := &column.descriptor
	if RetainedType(d.schema.dataType) != expected {
		return 0, false, &RetainedError{Code: RetainedUnsupportedType}
	}
	if position >= d.selected || d.selection.data == nil || position >= d.selection.length/4 {
		return 0, false, &RetainedError{Code: RetainedInvalidArgument}
	}
	physical := uint64(*(*uint32)(unsafe.Add(d.selection.data, uintptr(position)*4)))
	if physical >= d.physical || d.values.data == nil || physical >= d.values.length/8 {
		return 0, false, &RetainedError{Code: RetainedInvalidArgument}
	}
	if d.validityKind == 1 {
		return physical, true, nil
	}
	if d.validityKind != 2 || d.validity.data == nil || physical/64 >= d.validity.length/8 {
		return 0, false, &RetainedError{Code: RetainedInvalidArgument}
	}
	word := *(*uint64)(unsafe.Add(d.validity.data, uintptr(physical/64)*8))
	return physical, word&(uint64(1)<<(physical%64)) != 0, nil
}

// Int64At returns one selected scalar and its validity. A NULL returns zero
// and false; it is never silently cast to a non-null value.
func (column *RetainedColumn) Int64At(position uint64) (int64, bool, error) {
	column.owner.mu.RLock()
	defer column.owner.mu.RUnlock()
	return column.readInt64(position)
}

func (column *RetainedColumn) readInt64(position uint64) (int64, bool, error) {
	physical, valid, err := column.row(position, RetainedInt64)
	if err != nil || !valid {
		return 0, valid, err
	}
	value := *(*int64)(unsafe.Add(column.descriptor.values.data, uintptr(physical)*8))
	runtime.KeepAlive(column.owner)
	return value, true, nil
}

func (column *RetainedColumn) Float64At(position uint64) (float64, bool, error) {
	column.owner.mu.RLock()
	defer column.owner.mu.RUnlock()
	return column.readFloat64(position)
}

func (column *RetainedColumn) readFloat64(position uint64) (float64, bool, error) {
	physical, valid, err := column.row(position, RetainedFloat64)
	if err != nil || !valid {
		return 0, valid, err
	}
	value := *(*float64)(unsafe.Add(column.descriptor.values.data, uintptr(physical)*8))
	runtime.KeepAlive(column.owner)
	return value, true, nil
}

func (column *RetainedColumn) UInt64At(position uint64) (uint64, bool, error) {
	column.owner.mu.RLock()
	defer column.owner.mu.RUnlock()
	return column.readUInt64(position)
}

func (column *RetainedColumn) readUInt64(position uint64) (uint64, bool, error) {
	physical, valid, err := column.row(position, RetainedUInt64)
	if err != nil || !valid {
		return 0, valid, err
	}
	value := *(*uint64)(unsafe.Add(column.descriptor.values.data, uintptr(physical)*8))
	runtime.KeepAlive(column.owner)
	return value, true, nil
}
