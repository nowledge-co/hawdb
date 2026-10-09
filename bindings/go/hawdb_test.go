// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hawdb

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestGraph builds a small social graph and answers a friend-of-a-friend
// query — the traversal shape a graph database exists for:
//
//	alice -[:KNOWS]-> bob -[:KNOWS]-> carol -[:KNOWS]-> dave
func TestGraph(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "graph"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, stmt := range []string{
		"CREATE (:Person {name: 'alice'})",
		"CREATE (:Person {name: 'bob'})",
		"CREATE (:Person {name: 'carol'})",
		"CREATE (:Person {name: 'dave'})",
		`MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) CREATE (a)-[:KNOWS]->(b)`,
		`MATCH (b:Person {name: 'bob'}), (c:Person {name: 'carol'}) CREATE (b)-[:KNOWS]->(c)`,
		`MATCH (c:Person {name: 'carol'}), (d:Person {name: 'dave'}) CREATE (c)-[:KNOWS]->(d)`,
	} {
		if _, err := db.Query(stmt, nil); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// Friends of alice's friends: two hops out.
	res, err := db.Query(
		`MATCH (:Person {name: 'alice'})-[:KNOWS]->()-[:KNOWS]->(fof) RETURN fof.name AS name`,
		nil,
	)
	if err != nil {
		t.Fatalf("MATCH two-hop: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 friend-of-friend, got %v", res.Rows)
	}
	if name := res.Dicts()[0]["name"]; name != "carol" {
		t.Fatalf("expected carol, got %v", name)
	}
}

func TestSmoke(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "smoke"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Query(
		"CREATE (n:Person {name: $name, age: $age, avatar: $avatar, uid: $uid})",
		map[string]any{
			"name":   "alice",
			"age":    30,
			"avatar": []byte{0xDE, 0xAD, 0xBE, 0xEF},
			"uid":    UUID("550e8400-e29b-41d4-a716-446655440000"),
		},
	); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	res, err := db.Query(
		"MATCH (n:Person) RETURN n.name AS name, n.age AS age, n.avatar AS avatar, n.uid AS uid",
		nil,
	)
	if err != nil {
		t.Fatalf("MATCH: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d (columns=%v)", len(res.Rows), res.Columns)
	}
	dicts := res.Dicts()
	if dicts[0]["name"] != "alice" || dicts[0]["age"] != int64(30) {
		t.Fatalf("unexpected row: %v", dicts[0])
	}
	if avatar, ok := dicts[0]["avatar"].([]byte); !ok || len(avatar) != 4 || avatar[0] != 0xDE {
		t.Fatalf("binary round-trip failed: %v", dicts[0]["avatar"])
	}
	if uid, ok := dicts[0]["uid"].(UUID); !ok || uid != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("uuid round-trip failed: %v", dicts[0]["uid"])
	}
}

func TestShortestPath(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "path"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, stmt := range []string{
		"CREATE (:Person {id: 'alice', name: 'Alice'})",
		"CREATE (:Person {id: 'bob', name: 'Bob'})",
		"CREATE (:Person {id: 'carol', name: 'Carol'})",
		`MATCH (a:Person {id: 'alice'}), (b:Person {id: 'bob'}) CREATE (a)-[:KNOWS]->(b)`,
		`MATCH (b:Person {id: 'bob'}), (c:Person {id: 'carol'}) CREATE (b)-[:KNOWS]->(c)`,
	} {
		if _, err := db.Query(stmt, nil); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// HawDB's ALL SHORTEST syntax requires endpoint ids in WHERE.
	res, err := db.Query(
		`MATCH p = (a:Person)-[:KNOWS* ALL SHORTEST 1..3]->(b:Person)
		 WHERE a.id = $from AND b.id = $to
		 RETURN length(p) AS hops`,
		map[string]any{"from": "alice", "to": "carol"},
	)
	if err != nil {
		t.Fatalf("shortest path: %v", err)
	}
	dicts := res.Dicts()
	if len(dicts) != 1 || dicts[0]["hops"] != int64(2) {
		t.Fatalf("expected 2 hops, got %v", dicts)
	}
}

func TestSQL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "sql"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	res, err := db.QuerySQL("SELECT 1")
	if err != nil {
		t.Fatalf("QuerySQL: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestErrors(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "err"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Query("NOT VALID CYPHER AT ALL", nil); err == nil {
		t.Fatal("expected error for invalid cypher")
	} else {
		var herr *Error
		if !errors.As(err, &herr) {
			t.Fatalf("expected *hawdb.Error, got %T", err)
		}
		if herr.Message == "" {
			t.Fatal("empty error message")
		}
		if herr.Kind == "" {
			t.Fatal("empty error kind")
		}
	}
	if _, err := db.Query("RETURN 1", map[string]any{"x": func() {}}); err == nil {
		t.Fatal("expected marshal error")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := db.Query("RETURN 1", nil); err == nil {
		t.Fatal("expected error after Close")
	}
	if Version() == "" {
		t.Fatal("empty version")
	}
}

// TestLoadLibrary exercises the explicit library handle: same dylib,
// registered independently of the package-level default.
func TestLoadLibrary(t *testing.T) {
	path, err := findLibrary()
	if err != nil {
		t.Skipf("no library to load: %v", err)
	}
	lib, err := LoadLibrary(path)
	if err != nil {
		t.Fatalf("LoadLibrary: %v", err)
	}
	if lib.Version() == "" {
		t.Fatal("empty version")
	}
	db, err := lib.Open(filepath.Join(t.TempDir(), "via-lib"))
	if err != nil {
		t.Fatalf("lib.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Exec("CREATE (:Person {name: 'eleanor'})", nil); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	res, err := db.Query("MATCH (n:Person) RETURN n.name AS name", nil)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if name := res.Dicts()[0]["name"]; name != "eleanor" {
		t.Fatalf("expected eleanor, got %v", name)
	}
}

func TestLoadLibraryMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "libhawdb_ffi_missing.dylib")
	if _, err := LoadLibrary(missing); err == nil {
		t.Fatal("expected error loading a nonexistent library")
	}
}
