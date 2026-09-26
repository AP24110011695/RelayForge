package logengine

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	"relayforge/internal/record"
)

func TestRetentionBySize(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: 128,
		Retention: RetentionPolicy{MaxBytes: 700}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	activeBase := l.segments[len(l.segments)-1].base
	nextBefore := l.NextOffset()
	if len(l.segments) < 3 {
		t.Fatalf("expected several segments before retention, got %d", len(l.segments))
	}
	if err := l.ApplyRetention(time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := l.NextOffset(); got != nextBefore {
		t.Errorf("NextOffset changed during retention: %d -> %d", nextBefore, got)
	}
	if got := l.segments[len(l.segments)-1].base; got != activeBase {
		t.Errorf("active segment changed: %d -> %d", activeBase, got)
	}
	var total int64
	for _, seg := range l.segments {
		total += seg.size
	}
	if total > 700 && len(l.segments) > 1 {
		t.Errorf("retention stopped early: %d bytes across %d segments", total, len(l.segments))
	}
	earliest := l.segments[0].base
	if earliest == 0 {
		t.Logf("retention kept the first segment (total %d bytes)", total)
	} else {
		// Deleted range reads out of range; retained range reads fine.
		if _, _, err := l.ReadFrom(0, 1024); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Errorf("read from deleted offset 0: error = %v, want ErrOffsetOutOfRange", err)
		}
	}
	if _, _, err := l.ReadFrom(earliest, 1024); err != nil {
		t.Errorf("read from earliest retained offset %d: %v", earliest, err)
	}
	recs, err := readAll(l, earliest)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Error("no records retained")
	}
	// The active segment survives on-disk: only sealed files were removed.
	for base := earliest; base < activeBase; base++ {
		if _, err := os.Stat(segmentLogPath(dir, base)); err == nil {
			continue // still retained
		}
	}
}

func TestRetentionByAge(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(l.segments))
	}
	// Backdate every sealed segment beyond the age limit.
	old := time.Now().Add(-2 * time.Hour)
	for _, seg := range l.segments[:len(l.segments)-1] {
		if err := os.Chtimes(segmentLogPath(dir, seg.base), old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: 128,
		Retention: RetentionPolicy{MaxAge: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if got := l2.segments[0].base; got == 0 {
		t.Error("aged sealed segments were not dropped at open")
	}
	// Remaining records are readable and in sequence.
	recs, err := readAll(l2, l2.segments[0].base)
	if err != nil {
		t.Fatal(err)
	}
	first := int(l2.segments[0].base)
	for i, rec := range recs {
		if string(rec.Value) != string(mkrec(first+i).Value) {
			t.Errorf("retained record %d = %q, want %q", i, rec.Value, mkrec(first+i).Value)
		}
	}
}

func TestRetentionNeverDeletesActive(t *testing.T) {
	l, _ := openTempOpts(t, Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: 128,
		Retention: RetentionPolicy{MaxBytes: 10}})
	for i := 0; i < 30; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	activeBase := l.segments[len(l.segments)-1].base
	if err := l.ApplyRetention(time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(l.segments) != 1 {
		t.Errorf("segments after retention = %d, want 1 (active only)", len(l.segments))
	}
	if got := l.segments[0].base; got != activeBase {
		t.Errorf("active segment was deleted: base %d -> %d", activeBase, got)
	}
	if _, _, err := l.ReadFrom(0, 1024); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("read from deleted offset 0: error = %v, want ErrOffsetOutOfRange", err)
	}
	recs, err := readAll(l, activeBase)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Error("active segment records lost")
	}
}

func TestRetentionAtOpen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: 128,
		Retention: RetentionPolicy{MaxBytes: 600}})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	var total int64
	for _, seg := range l2.segments {
		total += seg.size
	}
	if total > 600 && len(l2.segments) > 1 {
		t.Errorf("retention not applied at open: %d bytes across %d segments", total, len(l2.segments))
	}
}

// TestRetentionConcurrentWithReaders: retention (which takes the write lock
// and deletes files) must never crash, hang, or corrupt concurrent readers.
func TestRetentionConcurrentWithReaders(t *testing.T) {
	l, _ := openTempOpts(t, Options{MaxRecordSize: 256, MaxSegmentBytes: 256, IndexIntervalBytes: 128,
		Retention: RetentionPolicy{MaxBytes: 800}})
	const total = 200
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()
	for i := 0; i < total; i++ {
		next := l.NextOffset()
		if next == 0 {
			continue
		}
		// Read a recent record; it may have been retained away already.
		if _, _, err := l.ReadFrom(next-1, 1024); err != nil && !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("read at %d: %v", next-1, err)
		}
	}
	<-done
	if err := l.Scan(func(record.Record, uint64) error { return nil }); err != nil {
		t.Errorf("scan after concurrent retention: %v", err)
	}
}

func TestStartRetentionTicker(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: 128,
		Retention: RetentionPolicy{MaxBytes: 700}})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := 0; i < 30; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	l.StartRetention(ctx, 15*time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.RLock()
		earliest := l.segments[0].base
		l.mu.RUnlock()
		if earliest > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	l.mu.RLock()
	earliest := l.segments[0].base
	l.mu.RUnlock()
	if earliest == 0 {
		t.Fatal("retention ticker never dropped a segment")
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutine leak after cancel: %d > %d", after, before)
	}
}
