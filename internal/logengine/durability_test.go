package logengine

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"relayforge/internal/record"
)

// isLogFile reports whether the wrapped file is a segment .log file (index
// files pass through unwrapped in the fault tests).
func isLogFile(f *os.File) bool {
	return strings.HasSuffix(f.Name(), logSuffix)
}

// The wrappers below sit on the segmentFile seam (see segment.go) and
// inject disk-class failures: ENOSPC, short writes, sync errors, sync
// latency, and read errors.

var errInjectedSync = errors.New("injected sync error")

// enospcOnce fails the first WriteAt AFTER it is armed, with a partial
// write + ENOSPC; otherwise it passes through. Wrappers start disarmed
// because the wrap callback also covers the initial segment at Open.
type enospcOnce struct {
	segmentFile
	armed atomic.Bool
}

func (f *enospcOnce) arm() { f.armed.Store(true) }

func (f *enospcOnce) WriteAt(p []byte, off int64) (int, error) {
	if f.armed.CompareAndSwap(true, false) {
		if _, err := f.segmentFile.WriteAt(p[:2], off); err != nil {
			return 0, err
		}
		return 2, syscall.ENOSPC
	}
	return f.segmentFile.WriteAt(p, off)
}

// shortWriteOnce completes only half of the first WriteAt after it is
// armed, returning a short count with nil error (a contract-violating
// writer); otherwise it passes through.
type shortWriteOnce struct {
	segmentFile
	armed atomic.Bool
}

func (f *shortWriteOnce) arm() { f.armed.Store(true) }

func (f *shortWriteOnce) WriteAt(p []byte, off int64) (int, error) {
	if f.armed.CompareAndSwap(true, false) {
		half := len(p) / 2
		if _, err := f.segmentFile.WriteAt(p[:half], off); err != nil {
			return 0, err
		}
		return half, nil
	}
	return f.segmentFile.WriteAt(p, off)
}

// syncFailures fails the first N Sync calls, then passes through.
type syncFailures struct {
	segmentFile
	remaining atomic.Int32
}

func newSyncFailures(f segmentFile, n int32) *syncFailures {
	w := &syncFailures{segmentFile: f}
	w.remaining.Store(n)
	return w
}

func (f *syncFailures) Sync() error {
	if f.remaining.Add(-1) >= 0 {
		return errInjectedSync
	}
	return f.segmentFile.Sync()
}

// syncDelay sleeps in every Sync call.
type syncDelay struct {
	segmentFile
	delay time.Duration
}

func (f *syncDelay) Sync() error {
	time.Sleep(f.delay)
	return f.segmentFile.Sync()
}

func TestAppendENOSPC(t *testing.T) {
	dir := t.TempDir()
	var wrapped atomic.Pointer[enospcOnce]
	l, err := openLog(dir, Options{}, func(base uint64, f *os.File) segmentFile {
		if !isLogFile(f) {
			return f
		}
		w := &enospcOnce{segmentFile: f}
		wrapped.Store(w)
		return w
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := l.Append(mkrec(0), DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	before := l.NextOffset()
	activePath := activeLogPath(l)
	sizeBefore := fileSize(t, activePath)

	// The injected ENOSPC must fail the append, consume no offset, leave
	// no partial bytes, and keep the log usable.
	wrapped.Load().arm()
	if _, err := l.Append(mkrec(1), DurabilityFlushed); err == nil {
		t.Fatal("append with injected ENOSPC succeeded")
	} else if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("error = %v, want ENOSPC", err)
	}
	if got := l.NextOffset(); got != before {
		t.Errorf("NextOffset advanced on failed append: %d -> %d", before, got)
	}
	if got := fileSize(t, activePath); got != sizeBefore {
		t.Errorf("partial bytes left behind: size %d, want %d", got, sizeBefore)
	}
	// Recovery: the next append (fault disarmed) continues the sequence.
	off, err := l.Append(mkrec(1), DurabilityWritten)
	if err != nil || off != 1 {
		t.Fatalf("append after ENOSPC: offset %d err %v; want 1, nil", off, err)
	}
	if err := l.Scan(func(record.Record, uint64) error { return nil }); err != nil {
		t.Errorf("scan after ENOSPC recovery: %v", err)
	}
}

func TestAppendShortWrite(t *testing.T) {
	dir := t.TempDir()
	var wrapped atomic.Pointer[shortWriteOnce]
	l, err := openLog(dir, Options{}, func(base uint64, f *os.File) segmentFile {
		if !isLogFile(f) {
			return f
		}
		w := &shortWriteOnce{segmentFile: f}
		wrapped.Store(w)
		return w
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := l.Append(mkrec(0), DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	before := l.NextOffset()
	activePath := activeLogPath(l)
	sizeBefore := fileSize(t, activePath)

	wrapped.Load().arm()
	_, err = l.Append(mkrec(1), DurabilityWritten)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want io.ErrShortWrite", err)
	}
	if got := l.NextOffset(); got != before {
		t.Errorf("NextOffset advanced on short write: %d -> %d", before, got)
	}
	if got := fileSize(t, activePath); got != sizeBefore {
		t.Errorf("partial bytes left behind: size %d, want %d", got, sizeBefore)
	}
	off, err := l.Append(mkrec(1), DurabilityWritten)
	if err != nil || off != 1 {
		t.Fatalf("append after short write: offset %d err %v; want 1, nil", off, err)
	}
}

func TestSyncErrorDegraded(t *testing.T) {
	dir := t.TempDir()
	l, err := openLog(dir, Options{}, func(base uint64, f *os.File) segmentFile {
		return newSyncFailures(f, 2)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Flushed append: offset consumed, error returned, degraded set.
	off, err := l.Append(mkrec(0), DurabilityFlushed)
	if err == nil {
		t.Fatal("flushed append with failing fsync succeeded")
	}
	if !errors.Is(err, errInjectedSync) {
		t.Fatalf("error = %v, want errInjectedSync", err)
	}
	if off != 0 {
		t.Errorf("offset = %d, want 0 (record is in the file even though the fsync failed)", off)
	}
	if !l.Degraded() {
		t.Error("Degraded() = false after fsync failure")
	}
	// The record itself is present and valid.
	recs, err := readAll(l, 0)
	if err != nil || len(recs) != 1 {
		t.Fatalf("read after failed fsync: %d records, err %v", len(recs), err)
	}
	// Written appends are unaffected by degradation.
	if _, err := l.Append(mkrec(1), DurabilityWritten); err != nil {
		t.Fatalf("written append while degraded: %v", err)
	}
	// Second flushed append: fsync fails again.
	if _, err := l.Append(mkrec(2), DurabilityFlushed); err == nil {
		t.Fatal("second flushed append succeeded while fsync still failing")
	}
	// Third: the wrapper's failures are exhausted; success clears degraded.
	if _, err := l.Append(mkrec(3), DurabilityFlushed); err != nil {
		t.Fatalf("flushed append after fsync recovery: %v", err)
	}
	if l.Degraded() {
		t.Error("Degraded() still true after a successful fsync")
	}
}

// TestFsyncBeforeReturnSeam is the ordering proof for the durability
// ladder: with an 80 ms injected sync cost, a Flushed append must pay it
// before returning and a Written append must not.
func TestFsyncBeforeReturnSeam(t *testing.T) {
	dir := t.TempDir()
	const delay = 80 * time.Millisecond
	l, err := openLog(dir, Options{}, func(base uint64, f *os.File) segmentFile {
		return &syncDelay{segmentFile: f, delay: delay}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	start := time.Now()
	if _, err := l.Append(mkrec(0), DurabilityFlushed); err != nil {
		t.Fatal(err)
	}
	flushed := time.Since(start)

	start = time.Now()
	if _, err := l.Append(mkrec(1), DurabilityWritten); err != nil {
		t.Fatal(err)
	}
	written := time.Since(start)

	if flushed < delay-time.Millisecond {
		t.Errorf("flushed append returned in %v; fsync (%v) did not happen before return", flushed, delay)
	}
	if written >= delay-time.Millisecond {
		t.Errorf("written append took %v; it must not pay the fsync cost", written)
	}
}
