package logengine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relayforge/internal/record"
)

var errInjectedIO = errors.New("injected I/O error")

// activeLogPath returns the active segment's log file path, for crash-
// simulation surgery in tests.
func activeLogPath(l *Log) string {
	base := l.segments[len(l.segments)-1].base
	return segmentLogPath(l.dir, base)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestTornTailRecovery(t *testing.T) {
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
	cleanSize := fileSize(t, activeLogPath(l))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: the first 10 bytes of a sixth record
	// land in the file.
	sixth, err := mkrec(5).Encode(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(activeLogPathOf(dir), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(sixth[:10], cleanSize); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer l2.Close()
	if got := l2.TruncatedOnRecover(); got != 10 {
		t.Errorf("TruncatedOnRecover = %d, want 10", got)
	}
	if got := l2.NextOffset(); got != 5 {
		t.Errorf("NextOffset = %d, want 5", got)
	}
	if size := fileSize(t, activeLogPath(l2)); size != cleanSize {
		t.Errorf("file size after recovery = %d, want %d", size, cleanSize)
	}
	off, err := l2.Append(mkrec(5), DurabilityWritten)
	if err != nil || off != 5 {
		t.Fatalf("append after recovery: offset %d err %v; want 5, nil", off, err)
	}
}

// activeLogPathOf returns the active segment path for a closed log (only
// one segment in these single-segment scenarios).
func activeLogPathOf(dir string) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", 0, logSuffix))
}

// TestRecoveryTruncationMatrix truncates the active segment at every byte
// boundary and requires the longest valid prefix to survive with offset
// continuity afterwards.
func TestRecoveryTruncationMatrix(t *testing.T) {
	dir := t.TempDir()
	path := activeLogPathOf(dir)
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		if _, err := l.Append(mkrec(i), DurabilityFlushed); err != nil {
			t.Fatal(err)
		}
	}
	pristine, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fullSize := int64(len(pristine))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	const recSize = int64(42) // mkrec: 26 header + 7 key + 9 value
	for cut := int64(0); cut < fullSize; cut++ {
		if err := os.WriteFile(path, pristine[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		l2, err := Open(dir, Options{})
		if err != nil {
			t.Fatalf("cut %d: reopen: %v", cut, err)
		}
		wantCount := uint64(cut / recSize)
		if got := l2.NextOffset(); got != wantCount {
			l2.Close()
			t.Fatalf("cut %d: NextOffset = %d, want %d", cut, got, wantCount)
		}
		if got := l2.TruncatedOnRecover(); got != cut-recSize*int64(wantCount) {
			l2.Close()
			t.Fatalf("cut %d: TruncatedOnRecover = %d, want %d", cut, got, cut-recSize*int64(wantCount))
		}
		if err := l2.Scan(func(record.Record, uint64) error { return nil }); err != nil {
			l2.Close()
			t.Fatalf("cut %d: scan: %v", cut, err)
		}
		off, err := l2.Append(mkrec(int(wantCount)), DurabilityWritten)
		if err != nil || off != wantCount {
			l2.Close()
			t.Fatalf("cut %d: append after recovery: offset %d err %v; want %d, nil", cut, off, err, wantCount)
		}
		if err := l2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, pristine, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCorruptTailRecovery(t *testing.T) {
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
	size := fileSize(t, activeLogPath(l))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the last record's value.
	f, err := os.OpenFile(activeLogPathOf(dir), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], size-3); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], size-3); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen with corrupt tail: %v", err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != 4 {
		t.Errorf("NextOffset = %d, want 4 (corrupt last record truncated)", got)
	}
	if got := l2.TruncatedOnRecover(); got == 0 {
		t.Error("TruncatedOnRecover = 0, want > 0")
	}
	if err := l2.Scan(func(record.Record, uint64) error { return nil }); err != nil {
		t.Errorf("scan after recovery: %v", err)
	}
}

// TestCorruptMiddlePolicy pins the documented policy: a corrupt record
// discards it AND everything after it.
func TestCorruptMiddlePolicy(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Ten records of exactly 100 bytes: record i spans [100*i, 100*i+100).
	for i := 0; i < 10; i++ {
		if _, err := l.Append(record.Record{Value: bytes.Repeat([]byte{byte(i)}, 74)}, DurabilityFlushed); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(activeLogPathOf(dir), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 100*1+30); err != nil { // inside record 1's value
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], 100*1+30); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != 1 {
		t.Errorf("NextOffset = %d, want 1 (truncate from first corrupt record)", got)
	}
	if got := l2.TruncatedOnRecover(); got != 100*9 {
		t.Errorf("TruncatedOnRecover = %d, want %d", got, 100*9)
	}
	off, err := l2.Append(record.Record{Value: []byte("fresh")}, DurabilityWritten)
	if err != nil || off != 1 {
		t.Errorf("append after recovery: offset %d err %v; want 1, nil", off, err)
	}
}

func TestGarbageActiveSegment(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(activeLogPathOf(dir), bytes.Repeat([]byte{0xFF}, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open garbage segment: %v", err)
	}
	defer l.Close()
	if got := l.NextOffset(); got != 0 {
		t.Errorf("NextOffset = %d, want 0", got)
	}
	if got := l.TruncatedOnRecover(); got != 64 {
		t.Errorf("TruncatedOnRecover = %d, want 64", got)
	}
	off, err := l.Append(record.Record{Value: []byte("ok")}, DurabilityWritten)
	if err != nil || off != 0 {
		t.Errorf("append into recovered garbage segment: offset %d err %v", off, err)
	}
}

// TestSealedCorruptionSurfacesOnScan pins the policy that sealed (immutable)
// history is never auto-truncated: Open does not scan sealed logs (only
// their indexes), so corruption there surfaces through Scan and reads —
// never through silent truncation.
func TestSealedCorruptionSurfacesOnScan(t *testing.T) {
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
	sealedPath := segmentLogPath(dir, l.segments[0].base)
	size := fileSize(t, sealedPath)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(sealedPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Open succeeds (the index looks consistent; sealed logs are not
	// scanned at open), but the corruption is never hidden.
	l2, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatalf("open with corrupt sealed log: %v", err)
	}
	defer l2.Close()
	err = l2.Scan(func(record.Record, uint64) error { return nil })
	if err == nil {
		t.Fatal("Scan accepted a corrupt sealed segment")
	}
	if got := fileSize(t, sealedPath); got != size {
		t.Errorf("sealed segment was auto-truncated: %d -> %d", size, got)
	}
	if got := l2.NextOffset(); got != 30 {
		t.Errorf("NextOffset changed: %d, want 30", got)
	}
}

// TestTransientReadErrorDoesNotTruncate: an I/O error during the recovery
// scan must fail Open without modifying the file.
func TestTransientReadErrorDoesNotTruncate(t *testing.T) {
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
	path := activeLogPath(l)
	size := fileSize(t, path)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = openLog(dir, Options{}, func(base uint64, f *os.File) segmentFile {
		return &failingReads{segmentFile: f}
	})
	if err == nil {
		t.Fatal("Open accepted an injected read error")
	}
	if !strings.Contains(err.Error(), "scanning active segment") {
		t.Errorf("error = %v, want the scan error path", err)
	}
	if got := fileSize(t, path); got != size {
		t.Errorf("file was modified by the failed open: %d -> %d", size, got)
	}
}

// failingReads fails every ReadAt (injected transient I/O error).
type failingReads struct{ segmentFile }

func (f failingReads) ReadAt(p []byte, off int64) (int, error) {
	return 0, errInjectedIO
}

// TestOffsetHoleDetectedByScan: manually deleting a middle segment must be
// caught by Scan's continuity check.
func TestOffsetHoleDetectedByScan(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err := l.Append(mkrec(i), DurabilityWritten); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(l.segments))
	}
	victim := l.segments[1].base
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segmentLogPath(dir, victim)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segmentIndexPath(dir, victim)); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatalf("Open with a hole (cheap checks pass): %v", err)
	}
	defer l2.Close()
	err = l2.Scan(func(record.Record, uint64) error { return nil })
	if !errors.Is(err, ErrOffsetHole) {
		t.Errorf("Scan error = %v, want ErrOffsetHole", err)
	}
}
