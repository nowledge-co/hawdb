// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

package hawdb

import (
	"io"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"unsafe"
)

const retainedNodeQuery = "MATCH (n:Item) WHERE n.id = $id RETURN n"

func nodeFixture(t *testing.T, persistent bool) *DB {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("retained node adapter is 64-bit only")
	}
	t.Helper()
	var db *DB
	var err error
	if persistent {
		db, err = Open(filepath.Join(t.TempDir(), "node.hawdb"), &OpenOptions{})
	} else {
		db, err = OpenInMemory()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, text := range []string{"CREATE NODE TABLE Item", "CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT", "CREATE INDEX ON :Item(id)"} {
		if err := db.Exec(text, nil); err != nil {
			t.Fatal(err)
		}
	}
	rows := make([]map[string]any, 128)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1)}
	}
	if err := db.Exec("UNWIND $rows AS row CREATE (:Item {id: row.id})", map[string]any{"rows": rows}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("MATCH (n:Item) WHERE n.id = $id SET n.payload = $payload, n.nested = $nested, n.score = $score, n.flag = $flag", map[string]any{"id": int64(1), "payload": "old\x00\u4e16", "nested": []any{map[string]any{"bytes": []byte{0, 255, 7}}}, "score": math.Copysign(0, -1), "flag": true}); err != nil {
		t.Fatal(err)
	}
	return db
}
func nodeCursor(t *testing.T, db *DB, id int64) *RetainedNodeCursor {
	t.Helper()
	cursor, err := db.QueryRetainedNode(retainedNodeQuery, map[string]any{"id": id}, RetainedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cursor.Close() })
	return cursor
}
func nodeNext(t *testing.T, cursor *RetainedNodeCursor) *RetainedNodeValue {
	t.Helper()
	value, err := cursor.Next()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { value.Close() })
	return value
}
func nodeField(t *testing.T, value *RetainedNodeValue, name string) *RetainedNodeValue {
	t.Helper()
	child, err := value.Field(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.Close() })
	return child
}
func nodeChild(t *testing.T, value *RetainedNodeValue, index uint64) *RetainedNodeValue {
	t.Helper()
	child, err := value.Child(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.Close() })
	return child
}
func nodeBytes(t *testing.T, value *RetainedNodeValue) []byte {
	t.Helper()
	length, err := value.ByteLen()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, length)
	for i := range out {
		out[i], err = value.ByteAt(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}
func TestRetainedNodeABIWidths(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("retained node adapter is 64-bit only")
	}
	if unsafe.Sizeof(retainedNodeDescriptor{}) != 72 || unsafe.Offsetof(retainedNodeDescriptor{}.data) != 40 {
		t.Fatal("incompatible retained node descriptor layout")
	}
}
func TestRetainedNodeSourceOwnership(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		name := "memory"
		if persistent {
			name = "file"
		}
		t.Run(name, func(t *testing.T) {
			db := nodeFixture(t, persistent)
			cursor := nodeCursor(t, db, 1)
			node := nodeNext(t, cursor)
			text := nodeField(t, node, "payload")
			duplicate := nodeField(t, node, "payload")
			retained, err := text.Retain()
			if err != nil {
				t.Fatal(err)
			}
			defer retained.Close()
			if text.descriptor.data != duplicate.descriptor.data || text.descriptor.data != retained.descriptor.data {
				t.Fatal("source address changed")
			}
			identity := nodeField(t, node, "_id")
			if _, err := identity.Int64(); err != nil {
				t.Fatal(err)
			}
			score := nodeField(t, node, "score")
			f, err := score.Float64()
			if err != nil || math.Float64bits(f) != math.Float64bits(math.Copysign(0, -1)) {
				t.Fatalf("float bits: %v %v", f, err)
			}
			flag := nodeField(t, node, "flag")
			b, err := flag.Bool()
			if err != nil || !b {
				t.Fatalf("bool: %v %v", b, err)
			}
			labels := nodeField(t, node, "labels")
			label := nodeChild(t, labels, 0)
			nested := nodeField(t, node, "nested")
			object := nodeChild(t, nested, 0)
			binary := nodeField(t, object, "bytes")
			if err := db.Exec("MATCH (n:Item) WHERE n.id = $id SET n.payload = 'new'", map[string]any{"id": int64(1)}); err != nil {
				t.Fatal(err)
			}
			fresh := nodeCursor(t, db, 1)
			freshNode := nodeNext(t, fresh)
			freshText := nodeField(t, freshNode, "payload")
			if string(nodeBytes(t, freshText)) != "new" {
				t.Fatal("new row not published")
			}
			freshText.Close()
			freshNode.Close()
			fresh.Close()
			if _, err := cursor.Next(); err != io.EOF {
				t.Fatalf("EOF: %v", err)
			}
			for _, value := range []*RetainedNodeValue{node, duplicate, text, identity, score, flag, labels, nested, object} {
				value.Close()
			}
			cursor.Close()
			db.Close()
			if string(nodeBytes(t, retained)) != "old\x00\u4e16" || string(nodeBytes(t, label)) != "Item" || string(nodeBytes(t, binary)) != string([]byte{0, 255, 7}) {
				t.Fatal("original payload changed after write/close")
			}
			state, err := retained.State()
			if err != nil || state.Status != 1 {
				t.Fatalf("completion: %+v %v", state, err)
			}
			retained.Close()
			if _, err := retained.ByteAt(0); err == nil {
				t.Fatal("closed owner readable")
			}
		})
	}
}
func TestRetainedNodeRefusalsAndEmptySeek(t *testing.T) {
	db := nodeFixture(t, false)
	empty := nodeCursor(t, db, 999)
	if _, err := empty.Next(); err != io.EOF {
		t.Fatalf("empty: %v", err)
	}
	empty.Close()
	if _, err := db.QueryRetainedNode(retainedNodeQuery, map[string]any{"id": 1}, RetainedOptions{Writable: true}); err == nil {
		t.Fatal("writable request accepted")
	} else {
		assertRetainedCode(t, err, RetainedCopyRequired)
	}
	if _, err := db.QueryRetainedNode("MATCH (n:Item) RETURN n", nil, RetainedOptions{}); err == nil {
		t.Fatal("scan accepted")
	} else {
		assertRetainedCode(t, err, RetainedUnsupportedPlan)
	}
	if _, err := db.QueryRetainedNode(retainedNodeQuery, map[string]any{"id": 1}, RetainedOptions{BatchBytes: 1}); err == nil {
		t.Fatal("tiny control accepted")
	} else {
		assertRetainedCode(t, err, RetainedWorkingUnitTooLarge)
	}
	cursor := nodeCursor(t, db, 1)
	node := nodeNext(t, cursor)
	if _, err := node.Field("missing"); err == nil {
		t.Fatal("missing field accepted")
	} else {
		assertRetainedCode(t, err, RetainedInvalidColumn)
	}
	scalar := nodeField(t, node, "id")
	if _, err := scalar.ByteAt(0); err == nil {
		t.Fatal("scalar exposed as bytes")
	}
	node.Close()
	cursor.Close()
	if v, err := scalar.Int64(); err != nil || v != 1 {
		t.Fatalf("independent field: %v %v", v, err)
	}
	state, err := scalar.State()
	if err != nil || state.Status != 3 {
		t.Fatalf("unfinished close: %+v %v", state, err)
	}
}
func TestRetainedNodeReadAndCloseConcurrent(t *testing.T) {
	db := nodeFixture(t, false)
	cursor := nodeCursor(t, db, 1)
	node := nodeNext(t, cursor)
	text := nodeField(t, node, "payload")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b, err := text.ByteAt(0)
				if err != nil {
					var code *RetainedError
					if e, ok := err.(*RetainedError); ok {
						code = e
					}
					if code == nil || code.Code != RetainedClosed {
						t.Errorf("read error: %v", err)
					}
					return
				}
				if b != 'o' {
					t.Errorf("byte: %v", b)
					return
				}
			}
		}()
	}
	text.Close()
	node.Close()
	cursor.Close()
	db.Close()
	wg.Wait()
}
