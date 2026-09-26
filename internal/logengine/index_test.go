package logengine

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"relayforge/internal/record"
)

// TestIndexLookupProperty: after random-sized appends across rolled
// segments, a read starting at EVERY offset must return exactly the right
// record first — the sparse index must never misroute.
func TestIndexLookupProperty(t *testing.T) {
	l, _ := openTempOpts(t, smallOpts())
	rnd := rand.New(rand.NewSource(7))
	var expected [][]byte
	for i := 0; i < 200; i++ {
		size := 1 + rnd.Intn(200)
		val := make([]byte, size)
		rnd.Read(val)
		expected = append(expected, val)
		if _, err := l.Append(record.Record{Value: val}, DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(l.segments))
	}
	for offset := uint64(0); offset < uint64(len(expected)); offset++ {
		recs, _, err := l.ReadFrom(offset, 1<<20)
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		if len(recs) == 0 {
			t.Fatalf("offset %d: empty batch", offset)
		}
		if !bytes.Equal(recs[0].Value, expected[offset]) {
			t.Fatalf("offset %d: read %d bytes, want %d bytes (misrouted)", offset, len(recs[0].Value), len(expected[offset]))
		}
	}
}

// TestIndexIntervalVariants: the index interval must not affect
// correctness, only lookup speed (sequential-scan fallback).
func TestIndexIntervalVariants(t *testing.T) {
	for _, interval := range []int64{1, 1 << 30} {
		t.Run(fmt.Sprintf("interval-%d", interval), func(t *testing.T) {
			l, _ := openTempOpts(t, Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: interval})
			for i := 0; i < 30; i++ {
				if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
					t.Fatal(err)
				}
			}
			recs, err := readAll(l, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != 30 {
				t.Fatalf("got %d records, want 30", len(recs))
			}
			for i, rec := range recs {
				if !bytes.Equal(rec.Value, mkrec(i).Value) {
					t.Errorf("record %d = %q", i, rec.Value)
				}
			}
		})
	}
}

// TestIndexCorruptionRecovery: corrupting, truncating, or deleting index
// files must never affect read correctness — indexes are rebuilt from the
// log, which is the source of truth.
func TestIndexCorruptionRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 4 {
		t.Fatalf("expected several segments, got %d", len(l.segments))
	}
	sealed := l.segments[:len(l.segments)-1]
	if len(sealed) < 3 {
		t.Fatalf("need at least 3 sealed segments, got %d", len(sealed))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Sealed index 0: flip a byte. Sealed index 1: truncate to 5 bytes.
	// Sealed index 2: delete entirely.
	flip := segmentIndexPath(dir, sealed[0].base)
	data, err := os.ReadFile(flip)
	if err != nil {
		t.Fatal(err)
	}
	data[8] ^= 0xFF
	if err := os.WriteFile(flip, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(segmentIndexPath(dir, sealed[1].base), 5); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segmentIndexPath(dir, sealed[2].base)); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatalf("reopen with damaged indexes: %v", err)
	}
	defer l2.Close()
	recs, err := readAll(l2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 50 {
		t.Fatalf("got %d records, want 50", len(recs))
	}
	for i, rec := range recs {
		if !bytes.Equal(rec.Value, mkrec(i).Value) {
			t.Errorf("record %d = %q", i, rec.Value)
		}
	}
	if err := l2.Scan(func(record.Record, uint64) error { return nil }); err != nil {
		t.Errorf("scan after index recovery: %v", err)
	}
}

// TestActiveIndexRewriteOnOpen: a damaged active index is rewritten from
// the recovery scan.
func TestActiveIndexRewriteOnOpen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d%s", 0, indexSuffix))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(indexPath, 3); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	info, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 3 || info.Size()%indexEntrySize != 0 || info.Size() < indexEntrySize {
		t.Errorf("active index was not rewritten: size %d", info.Size())
	}
	recs, err := readAll(l2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 20 {
		t.Fatalf("got %d records, want 20", len(recs))
	}
	if !bytes.Equal(recs[13].Value, mkrec(13).Value) {
		t.Errorf("record 13 = %q after index rewrite", recs[13].Value)
	}
}
