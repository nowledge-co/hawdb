// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

package hawdb

import (
	"fmt"
	"io"
	"testing"
)

const numericDeliveryQuery = "MATCH (n:Delivery) WHERE n.score >= $min RETURN n.score AS score, id(n) AS identity, n.score AS again"

func deliveryFixture(b *testing.B, rows int, persistent bool) *DB {
	b.Helper()
	var db *DB
	var err error
	if persistent {
		db, err = Open(b.TempDir())
	} else {
		db, err = OpenInMemory()
	}
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	for _, query := range []string{"CREATE NODE TABLE Delivery", "CREATE PROPERTY ON NODE TABLE Delivery(score) TYPE INT"} {
		if err := db.Exec(query, nil); err != nil {
			b.Fatal(err)
		}
	}
	for start := 0; start < rows; start += 512 {
		scores := make([]any, min(512, rows-start))
		for i := range scores {
			scores[i] = map[string]any{"score": int64(start + i)}
		}
		if err := db.Exec("UNWIND $rows AS row CREATE (:Delivery {score: row.score})", map[string]any{"rows": scores}); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

func consumeOwnedDelivery(db *DB) (uint64, int, error) {
	result, err := db.Query(numericDeliveryQuery, map[string]any{"min": int64(0)})
	if err != nil {
		return 0, 0, err
	}
	if len(result.Columns) != 3 {
		return 0, 0, fmt.Errorf("columns: %v", result.Columns)
	}
	hash := uint64(14695981039346656037)
	for _, row := range result.Rows {
		if len(row) != 3 {
			return 0, 0, fmt.Errorf("row width: %d", len(row))
		}
		for _, cell := range row {
			value, ok := cell.(int64)
			if !ok {
				return 0, 0, fmt.Errorf("numeric cell type: %T", cell)
			}
			hash = (hash ^ uint64(value)) * 1099511628211
		}
	}
	return hash, len(result.Rows), nil
}

func consumeRetainedDelivery(db *DB) (uint64, int, error) {
	cursor, err := db.QueryRetained(numericDeliveryQuery, map[string]any{"min": int64(0)}, RetainedOptions{})
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close()
	hash := uint64(14695981039346656037)
	count := 0
	for {
		batch, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		rows, err := batch.Rows()
		if err != nil {
			batch.Close()
			return 0, 0, err
		}
		for row := uint64(0); row < rows; row++ {
			a, av, ae := batch.Int64At(0, row)
			id, iv, ie := batch.UInt64At(1, row)
			c, cv, ce := batch.Int64At(2, row)
			if ae != nil || ie != nil || ce != nil || !av || !iv || !cv {
				batch.Close()
				return 0, 0, fmt.Errorf("scalar read: %v/%v/%v, validity %v/%v/%v", ae, ie, ce, av, iv, cv)
			}
			for _, value := range [...]uint64{uint64(a), id, uint64(c)} {
				hash = (hash ^ value) * 1099511628211
			}
			count++
		}
		if err := batch.Close(); err != nil {
			return 0, 0, err
		}
	}
	return hash, count, nil
}

// Setup is excluded; each timed iteration includes query, delivery, scalar
// checksum and retained release. Default memory/durability limits are preserved.
func BenchmarkNumericResultDelivery(b *testing.B) {
	for _, rows := range []int{1000, 10000, 100000} {
		for _, backend := range []string{"memory", "file"} {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, backend), func(b *testing.B) {
				db := deliveryFixture(b, rows, backend == "file")
				expected, count, err := consumeOwnedDelivery(db)
				if err != nil || count != rows {
					b.Fatalf("ordinary baseline: rows=%d err=%v", count, err)
				}
				for _, mode := range []struct {
					name    string
					consume func(*DB) (uint64, int, error)
				}{{"owned", consumeOwnedDelivery}, {"retained", consumeRetainedDelivery}} {
					b.Run(mode.name, func(b *testing.B) {
						// Discard one full iteration for both representations.
						checksum, count, err := mode.consume(db)
						if err != nil || count != rows || checksum != expected {
							b.Fatalf("warmup parity: rows=%d checksum=%x err=%v", count, checksum, err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							checksum, count, err := mode.consume(db)
							if err != nil || count != rows || checksum != expected {
								b.Fatalf("parity: rows=%d checksum=%x err=%v", count, checksum, err)
							}
						}
					})
				}
			})
		}
	}
}
