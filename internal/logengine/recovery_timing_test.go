package logengine

// Recovery-time measurement: build a multi-megabyte log, close it, and
// time the Open recovery scan. Committed as a test so the number in
// docs/BENCHMARKS.md is reproducible (`go test -run TestRecoveryTiming -v`).
// NOTE: it measures the Open scan on an already-closed, clean log — the
// crash-free recovery path — on the local ext4 filesystem.

import (
	"fmt"
	"testing"
	"time"

	"relayforge/internal/record"
)

func TestRecoveryTiming(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxRecordSize: 1 << 20, MaxSegmentBytes: 8 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	rec := record.Record{Value: make([]byte, 1024)}
	const total = 24000 // ~24 MB
	for i := 0; i < total; i++ {
		if _, err := l.Append(rec, DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	l2, err := Open(dir, Options{MaxRecordSize: 1 << 20, MaxSegmentBytes: 8 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if got := l2.NextOffset(); got != total {
		t.Fatalf("recovered %d records, want %d", got, total)
	}
	if l2.TruncatedOnRecover() != 0 {
		t.Fatalf("clean log truncated: %d bytes", l2.TruncatedOnRecover())
	}
	l2.Close()
	fmt.Printf("recovery: %d records (~24 MB) scanned in %v (%.1f MB/s)\n",
		total, elapsed, float64(24)/(elapsed.Seconds()))
}
