// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

package hawdb

import (
	"encoding/json"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// RetainedNodeKind distinguishes source-backed row fields and containers.
type RetainedNodeKind uint32

const (
	RetainedNodeObject  RetainedNodeKind = 1
	RetainedNodeNull    RetainedNodeKind = 2
	RetainedNodeBool    RetainedNodeKind = 3
	RetainedNodeInt64   RetainedNodeKind = 4
	RetainedNodeFloat64 RetainedNodeKind = 5
	RetainedNodeUTF8    RetainedNodeKind = 6
	RetainedNodeBinary  RetainedNodeKind = 7
	RetainedNodeUUID    RetainedNodeKind = 8
	RetainedNodeList    RetainedNodeKind = 9
	RetainedNodeMap     RetainedNodeKind = 10
)

type retainedNodeDescriptor struct {
	version, size uint32
	namespace, id uint64
	kind, flags   uint32
	children      uint64
	data          unsafe.Pointer
	length        uint64
	name          *byte
	nameLen       uint64
}
type retainedNodeFunctions struct {
	query        func(unsafe.Pointer, *retainedRequest, uint32, *retainedCursorDescriptor, uint32) uint32
	next, retain func(uint64, uint64, *retainedNodeDescriptor, uint32) uint32
	child        func(uint64, uint64, uint64, *retainedNodeDescriptor, uint32) uint32
	field        func(uint64, uint64, *byte, uint64, *retainedNodeDescriptor, uint32) uint32
}

func loadRetainedNodeFunctions(handle uintptr) *retainedNodeFunctions {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		return nil
	}
	names := [...]string{"hawdb_retained_node_query", "hawdb_retained_node_next", "hawdb_retained_node_retain", "hawdb_retained_node_child", "hawdb_retained_node_field"}
	var symbols [len(names)]uintptr
	for i, name := range names {
		symbol, err := lookupSharedSymbol(handle, name)
		if err != nil {
			return nil
		}
		symbols[i] = symbol
	}
	f := &retainedNodeFunctions{}
	purego.RegisterFunc(&f.query, symbols[0])
	purego.RegisterFunc(&f.next, symbols[1])
	purego.RegisterFunc(&f.retain, symbols[2])
	purego.RegisterFunc(&f.child, symbols[3])
	purego.RegisterFunc(&f.field, symbols[4])
	return f
}

// RetainedNodeCursor actively pulls one eligible indexed node. Results remain
// provisional until EOF. Closing does not revoke independently owned values.
type RetainedNodeCursor struct{ owner *retainedOwner }

// QueryRetainedNode delivers immutable original row storage without JSON result
// encoding or Arrow repacking. Only a real single-match materialized index plan
// qualifies; unsupported plans/layouts and writable requests explicitly refuse.
// Parameters still use JSON input. No whole-process RSS bound is promised.
func (db *DB) QueryRetainedNode(cypher string, params map[string]any, options RetainedOptions) (*RetainedNodeCursor, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, &RetainedError{Code: RetainedClosed}
	}
	if db.lib.retained == nil || db.lib.retainedNode == nil {
		return nil, &Error{Kind: "capability_unavailable", Message: "retained node ABI v1 requires a compatible library and 64-bit host"}
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
	request := retainedRequest{version: 1, size: uint32(unsafe.Sizeof(retainedRequest{})), text: textPtr, textLen: uint64(textLen), params: paramsPtr, paramsLen: uint64(paramsLen), rows: options.BatchRows, slots: options.OutstandingBatches, bytes: options.BatchBytes, flags: 1}
	if options.Writable {
		request.flags |= 2
	}
	var descriptor retainedCursorDescriptor
	code := db.lib.retainedNode.query(db.ptr, &request, request.size, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	runtime.KeepAlive(cypher)
	runtime.KeepAlive(paramsJSON)
	runtime.KeepAlive(db)
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return &RetainedNodeCursor{owner: newRetainedOwner(db.lib, descriptor.namespace, descriptor.id)}, nil
}
func (cursor *RetainedNodeCursor) Next() (*RetainedNodeValue, error) {
	owner := cursor.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedNodeDescriptor
	code := owner.lib.retainedNode.next(owner.namespace, owner.id, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return nodeValue(owner.lib, descriptor), nil
}
func (cursor *RetainedNodeCursor) Close() error                  { return cursor.owner.close() }
func (cursor *RetainedNodeCursor) State() (RetainedState, error) { return cursor.owner.state() }

// RetainedNodeValue owns one immutable source view. Go value copies share its
// release state; Retain creates an independently admitted owner. No mutable
// slice or pointer escapes a read lock. ByteAt and scalar readers load directly
// from original native storage; NameCopy explicitly copies UTF-8 metadata.
type RetainedNodeValue struct {
	owner      *retainedOwner
	descriptor retainedNodeDescriptor
}

func nodeValue(lib *Library, descriptor retainedNodeDescriptor) *RetainedNodeValue {
	return &RetainedNodeValue{owner: newRetainedOwner(lib, descriptor.namespace, descriptor.id), descriptor: descriptor}
}
func (value *RetainedNodeValue) Close() error                  { return value.owner.close() }
func (value *RetainedNodeValue) State() (RetainedState, error) { return value.owner.state() }
func (value *RetainedNodeValue) Kind() (RetainedNodeKind, error) {
	value.owner.mu.RLock()
	defer value.owner.mu.RUnlock()
	if err := value.owner.check(); err != nil {
		return 0, err
	}
	return RetainedNodeKind(value.descriptor.kind), nil
}
func (value *RetainedNodeValue) ChildCount() (uint64, error) {
	value.owner.mu.RLock()
	defer value.owner.mu.RUnlock()
	if err := value.owner.check(); err != nil {
		return 0, err
	}
	return value.descriptor.children, nil
}
func (value *RetainedNodeValue) ByteLen() (uint64, error) {
	value.owner.mu.RLock()
	defer value.owner.mu.RUnlock()
	if err := value.owner.check(); err != nil {
		return 0, err
	}
	return value.descriptor.length, nil
}
func (value *RetainedNodeValue) NameCopy() (string, error) {
	value.owner.mu.RLock()
	defer value.owner.mu.RUnlock()
	if err := value.owner.check(); err != nil {
		return "", err
	}
	d := value.descriptor
	if d.nameLen > uint64(^uint(0)>>1) || (d.nameLen != 0 && d.name == nil) {
		return "", &RetainedError{Code: RetainedInvalidArgument}
	}
	name := string(unsafe.Slice(d.name, int(d.nameLen)))
	runtime.KeepAlive(value.owner)
	return name, nil
}
func (value *RetainedNodeValue) Retain() (*RetainedNodeValue, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedNodeDescriptor
	code := owner.lib.retainedNode.retain(owner.namespace, owner.id, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return nodeValue(owner.lib, descriptor), nil
}

// Child indexes lists, sorted maps, or node fields (_id, labels, then sorted
// properties). Its owner remains readable after all ancestors close.
func (value *RetainedNodeValue) Child(index uint64) (*RetainedNodeValue, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	var descriptor retainedNodeDescriptor
	code := owner.lib.retainedNode.child(owner.namespace, owner.id, index, &descriptor, uint32(unsafe.Sizeof(descriptor)))
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return nodeValue(owner.lib, descriptor), nil
}

// Field looks up a node/map key without creating a foreign container. Missing
// fields and scalar inputs return RetainedInvalidColumn with no owner.
func (value *RetainedNodeValue) Field(name string) (*RetainedNodeValue, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return nil, err
	}
	ptr, length := strArg(name)
	var descriptor retainedNodeDescriptor
	code := owner.lib.retainedNode.field(owner.namespace, owner.id, ptr, uint64(length), &descriptor, uint32(unsafe.Sizeof(descriptor)))
	runtime.KeepAlive(name)
	if err := retainedError(code); err != nil {
		return nil, err
	}
	return nodeValue(owner.lib, descriptor), nil
}
func (value *RetainedNodeValue) ByteAt(index uint64) (byte, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return 0, err
	}
	d := value.descriptor
	if d.kind != uint32(RetainedNodeUTF8) && d.kind != uint32(RetainedNodeBinary) && d.kind != uint32(RetainedNodeUUID) {
		return 0, &RetainedError{Code: RetainedUnsupportedType}
	}
	if index >= d.length || index > uint64(^uint(0)>>1) || d.data == nil {
		return 0, &RetainedError{Code: RetainedInvalidArgument}
	}
	result := *(*byte)(unsafe.Add(d.data, uintptr(index)))
	runtime.KeepAlive(owner)
	return result, nil
}
func (value *RetainedNodeValue) Int64() (int64, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return 0, err
	}
	if value.descriptor.kind != uint32(RetainedNodeInt64) {
		return 0, &RetainedError{Code: RetainedUnsupportedType}
	}
	result := *(*int64)(value.descriptor.data)
	runtime.KeepAlive(owner)
	return result, nil
}
func (value *RetainedNodeValue) Float64() (float64, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return 0, err
	}
	if value.descriptor.kind != uint32(RetainedNodeFloat64) {
		return 0, &RetainedError{Code: RetainedUnsupportedType}
	}
	result := *(*float64)(value.descriptor.data)
	runtime.KeepAlive(owner)
	return result, nil
}
func (value *RetainedNodeValue) Bool() (bool, error) {
	owner := value.owner
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	if err := owner.check(); err != nil {
		return false, err
	}
	if value.descriptor.kind != uint32(RetainedNodeBool) {
		return false, &RetainedError{Code: RetainedUnsupportedType}
	}
	result := *(*byte)(value.descriptor.data) != 0
	runtime.KeepAlive(owner)
	return result, nil
}
