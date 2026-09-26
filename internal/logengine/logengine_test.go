package logengine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"relayforge/internal/record"
)

var errScanAbort = errors.New("scan aborted by test")

// smallOpts produces several segments quickly: ~40-byte records roll every
// ~12 records and index every ~3 records.
func smallOpts() Options {
	return Options{MaxRecordSize: 256, MaxSegmentBytes: 512, IndexIntervalBytes: 128}
}

func openTemp(t *testing.T) (*Log, string) {
	t.Helper()
	return openTempOpts(t, Options{})
}

func openTempOpts(t *testing.T, opts Options) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, dir
}

// mkrec builds a record with a distinct value and a fixed timestamp (so
// broker-stamped timestamps can be tested separately).
func mkrec(i int) record.Record {
	return record.Record{
		Key:       []byte(fmt.Sprintf("key-%03d", i%3)),
		Value:     []byte(fmt.Sprintf("value-%03d", i)),
		Timestamp: int64(1_000_000 + i),
	}
}

// readAll collects every record from offset `from` to the current end by
// following next-offsets across segment boundaries.
func readAll(l *Log, from uint64) ([]record.Record, error) {
	var out []record.Record
	cursor := from
	for {
		recs, next, err := l.ReadFrom(cursor, 1<<20)
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
		if next == cursor && len(recs) == 0 {
			return out, nil
		}
		cursor = next
	}
}

func TestOpenEmptyLog(t *testing.T) {
	l, _ := openTemp(t)
	if got := l.NextOffset(); got != 0 {
		t.Errorf("NextOffset = %d, want 0", got)
	}
	if _, ok := l.LastOffset(); ok {
		t.Error("LastOffset on empty log reported an offset")
	}
	recs, next, err := l.ReadFrom(0, 1024)
	if err != nil {
		t.Fatalf("ReadFrom at end: %v", err)
	}
	if len(recs) != 0 || next != 0 {
		t.Errorf("at-end read returned %d records, next %d; want empty, 0", len(recs), next)
	}
	if _, _, err := l.ReadFrom(1, 1024); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("ReadFrom beyond end: error = %v, want ErrOffsetOutOfRange", err)
	}
}

func TestAppendRead(t *testing.T) {
	l, _ := openTemp(t)
	for i := 0; i < 10; i++ {
		off, err := l.Append(mkrec(i), DurabilityWritten)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Fatalf("append %d got offset %d", i, off)
		}
	}
	if got := l.NextOffset(); got != 10 {
		t.Errorf("NextOffset = %d, want 10", got)
	}
	if last, ok := l.LastOffset(); !ok || last != 9 {
		t.Errorf("LastOffset = (%d, %v), want (9, true)", last, ok)
	}

	recs, next, err := l.ReadFrom(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 || next != 10 {
		t.Fatalf("full read: %d records, next %d; want 10, 10", len(recs), next)
	}
	for i, rec := range recs {
		if !bytes.Equal(rec.Value, mkrec(i).Value) || !bytes.Equal(rec.Key, mkrec(i).Key) {
			t.Errorf("record %d: got key=%q value=%q", i, rec.Key, rec.Value)
		}
		if rec.Timestamp != int64(1_000_000+i) {
			t.Errorf("record %d: timestamp %d, want %d", i, rec.Timestamp, 1_000_000+i)
		}
	}

	recs, next, err = l.ReadFrom(4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 6 || next != 10 {
		t.Fatalf("mid read: %d records, next %d; want 6, 10", len(recs), next)
	}
	if !bytes.Equal(recs[0].Value, []byte("value-004")) {
		t.Errorf("mid read first value = %q, want value-004", recs[0].Value)
	}

	if _, _, err := l.ReadFrom(10, 1024); err != nil {
		t.Errorf("at-end read: %v", err)
	}
	if _, _, err := l.ReadFrom(11, 1024); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("beyond-end read: error = %v, want ErrOffsetOutOfRange", err)
	}
	if _, _, err := l.ReadFrom(0, 0); !errors.Is(err, ErrInvalidMaxBytes) {
		t.Errorf("maxBytes=0: error = %v, want ErrInvalidMaxBytes", err)
	}
}

func TestMaxBytes(t *testing.T) {
	l, _ := openTemp(t)
	// Ten records of exactly 100 bytes each (26 header + 74 value).
	for i := 0; i < 10; i++ {
		rec := record.Record{Value: bytes.Repeat([]byte{byte(i)}, 74)}
		if _, err := l.Append(rec, DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	recs, next, err := l.ReadFrom(0, 350)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || next != 3 {
		t.Errorf("maxBytes=350: %d records, next %d; want 3, 3", len(recs), next)
	}
	// At least one record is returned even when it alone exceeds maxBytes.
	recs, _, err = l.ReadFrom(0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Errorf("maxBytes=50: %d records, want 1 (at-least-one rule)", len(recs))
	}
}

func TestTimestampStamping(t *testing.T) {
	l, _ := openTemp(t)
	if _, err := l.Append(record.Record{Value: []byte("stamped")}, DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(record.Record{Value: []byte("explicit"), Timestamp: 1234567890123}, DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	recs, _, err := l.ReadFrom(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if recs[0].Timestamp == 0 {
		t.Error("zero timestamp was not stamped at append")
	}
	if recs[0].Timestamp < time.Now().UnixMilli()-60_000 {
		t.Errorf("stamped timestamp %d is not close to now", recs[0].Timestamp)
	}
	if recs[1].Timestamp != 1234567890123 {
		t.Errorf("explicit timestamp changed: %d", recs[1].Timestamp)
	}
}

func TestReopenPersists(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := l.Append(mkrec(i), DurabilityFlushed); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != 5 {
		t.Fatalf("after reopen NextOffset = %d, want 5", got)
	}
	if got := l2.TruncatedOnRecover(); got != 0 {
		t.Errorf("TruncatedOnRecover = %d, want 0", got)
	}
	var got []string
	err = l2.Scan(func(rec record.Record, offset uint64) error {
		if uint64(len(got)) != offset {
			t.Errorf("scan offset %d out of order", offset)
		}
		got = append(got, string(rec.Value))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if got[i] != fmt.Sprintf("value-%03d", i) {
			t.Errorf("record %d = %q", i, got[i])
		}
	}
	// Offset continuity: the next append continues the sequence.
	off, err := l2.Append(mkrec(5), DurabilityWritten)
	if err != nil || off != 5 {
		t.Errorf("append after reopen: offset %d err %v; want 5, nil", off, err)
	}
}

func TestSealedSegmentsNeverAppended(t *testing.T) {
	l, _ := openTempOpts(t, smallOpts())
	for i := 0; i < 30; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(l.segments))
	}
	// Record the size of every segment that is sealed now.
	sealedSizes := make(map[uint64]int64)
	for _, seg := range l.segments {
		if seg.sealed {
			sealedSizes[seg.base] = seg.size
		}
	}
	if len(sealedSizes) == 0 {
		t.Fatal("no sealed segments to check")
	}
	for i := 30; i < 60; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	for _, seg := range l.segments {
		if want, ok := sealedSizes[seg.base]; ok && seg.sealed {
			if seg.size != want {
				t.Errorf("sealed segment %d changed size: %d -> %d", seg.base, want, seg.size)
			}
		}
	}
	if err := l.Scan(func(record.Record, uint64) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestReadAcrossSegments(t *testing.T) {
	l, _ := openTempOpts(t, smallOpts())
	for i := 0; i < 20; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := readAll(l, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 20 {
		t.Fatalf("readAll got %d records, want 20", len(recs))
	}
	for i, rec := range recs {
		if !bytes.Equal(rec.Value, mkrec(i).Value) {
			t.Errorf("record %d = %q", i, rec.Value)
		}
	}
	// A read starting in a later segment routes correctly.
	recs, _, err = l.ReadFrom(13, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 || !bytes.Equal(recs[0].Value, []byte("value-013")) {
		t.Errorf("read from offset 13 starts at %v", recs)
	}
}

func TestConcurrentAppends(t *testing.T) {
	l, _ := openTemp(t)
	const workers, perWorker = 8, 25
	offsets := make([]uint64, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				rec := record.Record{Value: []byte(fmt.Sprintf("w%d-m%03d", id, i))}
				off, err := l.Append(rec, DurabilityWritten)
				if err != nil {
					t.Errorf("worker %d append %d: %v", id, i, err)
					return
				}
				offsets[id*perWorker+i] = off
			}
		}(w)
	}
	wg.Wait()

	if got := l.NextOffset(); got != workers*perWorker {
		t.Fatalf("NextOffset = %d, want %d", got, workers*perWorker)
	}
	seen := make(map[uint64]bool)
	for _, off := range offsets {
		if seen[off] {
			t.Errorf("duplicate offset %d", off)
		}
		seen[off] = true
	}
	values := make(map[string]bool)
	err := l.Scan(func(rec record.Record, offset uint64) error {
		if !seen[offset] {
			t.Errorf("record at unexpected offset %d", offset)
		}
		if values[string(rec.Value)] {
			t.Errorf("duplicate value %q", rec.Value)
		}
		values[string(rec.Value)] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != workers*perWorker {
		t.Errorf("scanned %d distinct records, want %d", len(values), workers*perWorker)
	}
}

// TestConcurrentReadersDuringAppends: readers must only ever observe
// complete records while a writer appends, across segment rolls.
func TestConcurrentReadersDuringAppends(t *testing.T) {
	l, _ := openTempOpts(t, smallOpts())
	const total = 150
	deadline := time.Now().Add(10 * time.Second)

	var writerDone sync.WaitGroup
	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		for i := 0; i < total; i++ {
			if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func(id int) {
			defer readers.Done()
			previous := 0
			for {
				if time.Now().After(deadline) {
					t.Errorf("reader %d: timed out waiting for %d records", id, total)
					return
				}
				recs, err := readAll(l, 0)
				if err != nil {
					t.Errorf("reader %d: %v", id, err)
					return
				}
				if len(recs) < previous {
					t.Errorf("reader %d: record count went backwards: %d < %d", id, len(recs), previous)
					return
				}
				previous = len(recs)
				if len(recs) == total {
					for i, rec := range recs {
						if !bytes.Equal(rec.Value, mkrec(i).Value) {
							t.Errorf("reader %d: record %d = %q", id, i, rec.Value)
							return
						}
					}
					return
				}
			}
		}(r)
	}
	writerDone.Wait()
	readers.Wait()

	if got := l.NextOffset(); got != total {
		t.Errorf("NextOffset = %d, want %d", got, total)
	}
}

func TestScanAbort(t *testing.T) {
	l, _ := openTemp(t)
	for i := 0; i < 3; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	err := l.Scan(func(record.Record, uint64) error { return errScanAbort })
	if !errors.Is(err, errScanAbort) {
		t.Errorf("Scan abort: error = %v, want errScanAbort", err)
	}
}

func TestClosedLog(t *testing.T) {
	l, _ := openTemp(t)
	if _, err := l.Append(mkrec(0), DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(mkrec(1), DurabilityWritten); !errors.Is(err, ErrClosed) {
		t.Errorf("Append after close: error = %v, want ErrClosed", err)
	}
	if _, _, err := l.ReadFrom(0, 1024); !errors.Is(err, ErrClosed) {
		t.Errorf("ReadFrom after close: error = %v, want ErrClosed", err)
	}
	if err := l.Scan(func(record.Record, uint64) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("Scan after close: error = %v, want ErrClosed", err)
	}
	if err := l.ApplyRetention(time.Now()); !errors.Is(err, ErrClosed) {
		t.Errorf("ApplyRetention after close: error = %v, want ErrClosed", err)
	}
	if err := l.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close: error = %v, want ErrClosed", err)
	}
}

func TestOffsetValidation(t *testing.T) {
	l, _ := openTemp(t)
	if _, err := l.Append(mkrec(0), DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	// MaxRecordSize below the minimum usable size is rejected at Open.
	if _, err := Open(t.TempDir(), Options{MaxRecordSize: -1}); err == nil {
		t.Error("negative MaxRecordSize accepted")
	}
	// MaxSegmentBytes below MaxRecordSize is rejected at Open.
	if _, err := Open(t.TempDir(), Options{MaxRecordSize: 2048, MaxSegmentBytes: 1024}); err == nil {
		t.Error("MaxSegmentBytes < MaxRecordSize accepted")
	}
}

func BenchmarkLogAppend(b *testing.B) {
	dir := b.TempDir()
	l, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	rec := record.Record{Key: []byte("bench"), Value: make([]byte, 1024)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Append(rec, DurabilityWritten); err != nil {
			b.Fatal(err)
		}
	}
}
