// Package logengine implements RelayForge's single-file append-only log.
//
// Offsets are per-log, 0-based, contiguous sequence numbers assigned in
// append order — they are not file positions. An in-memory offset→position
// table (rebuilt by scanning on Open) maps them for reads; it is a documented
// temporary device until segments and a sparse index arrive in Phase 4.
//
// Concurrency model for v1: one RWMutex; appends take the write lock and
// complete their write before releasing it, so readers (read lock) only ever
// see fully written records. Reads use position-based ReadAt and never touch
// the file offset.
package logengine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"relayforge/internal/record"
)

// DefaultMaxRecordSize is the default per-record cap, applied to the full
// encoded size (record.HeaderSize + key + value).
const DefaultMaxRecordSize = 4 << 20

// Options configures Open. Zero fields take defaults.
type Options struct {
	// MaxRecordSize caps the full encoded size of one record.
	// 0 selects DefaultMaxRecordSize.
	MaxRecordSize int
}

// Errors returned by log operations.
var (
	// ErrOffsetOutOfRange means the requested offset lies beyond the end
	// of the log. Reading exactly at NextOffset is the at-end case and
	// returns an empty batch instead.
	ErrOffsetOutOfRange = errors.New("logengine: offset out of range")
	// ErrInvalidMaxBytes means a ReadFrom batch limit is not positive.
	ErrInvalidMaxBytes = errors.New("logengine: maxBytes must be positive")
	// ErrClosed is returned by operations after Close, and by a second
	// Close.
	ErrClosed = errors.New("logengine: log is closed")
)

// Log is a single-file append-only log. It is safe for concurrent use:
// one writer at a time (serialized internally), many concurrent readers.
type Log struct {
	mu        sync.RWMutex
	file      *os.File
	path      string
	maxSize   int
	positions []int64 // offset -> file position; len(positions) == next offset
	end       int64   // current file size
	truncated int64   // bytes discarded by recovery at Open
	closed    bool
}

// Open opens (or creates) the log file at path — the parent directory must
// exist — and recovers it to the longest valid prefix: from the first
// invalid record onward (torn tail or corruption), everything is truncated.
// A transient I/O error during the scan is not treated as corruption: Open
// fails without modifying the file.
func Open(path string, opts Options) (*Log, error) {
	maxSize := opts.MaxRecordSize
	if maxSize == 0 {
		maxSize = DefaultMaxRecordSize
	}
	if maxSize < 0 {
		return nil, fmt.Errorf("logengine: MaxRecordSize %d must not be negative", maxSize)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{file: f, path: path, maxSize: maxSize}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover scans the file from position 0, records one position per valid
// record, and truncates from the first invalid record onward.
func (l *Log) recover() error {
	info, err := l.file.Stat()
	if err != nil {
		return err
	}
	r := io.NewSectionReader(l.file, 0, info.Size())
	var positions []int64
	var pos int64
	for {
		_, n, err := record.DecodeFrom(r, l.maxSize)
		if err == nil {
			positions = append(positions, pos)
			pos += int64(n)
			continue
		}
		switch {
		case errors.Is(err, io.EOF):
			// clean end of the valid prefix
		case isCorruption(err):
			// torn tail or corruption: the documented policy discards
			// everything from the first invalid record onward
		default:
			// a transient read error must not cause truncation
			return fmt.Errorf("logengine: scanning %s at position %d: %w", l.path, pos, err)
		}
		if pos < info.Size() {
			if err := l.file.Truncate(pos); err != nil {
				return fmt.Errorf("logengine: truncating invalid tail of %s at %d: %w", l.path, pos, err)
			}
			l.truncated = info.Size() - pos
		}
		break
	}
	l.positions = positions
	l.end = pos
	return nil
}

// isCorruption reports whether err is a record-decoding failure that marks
// the log content as invalid from this point onward.
func isCorruption(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, record.ErrChecksumMismatch) ||
		errors.Is(err, record.ErrUnsupportedVersion) ||
		errors.Is(err, record.ErrReservedAttributes) ||
		errors.Is(err, record.ErrMalformed) ||
		errors.Is(err, record.ErrRecordTooLarge)
}

// Append validates and writes one record, returning its assigned offset.
// A zero Timestamp is stamped with the current wall clock (milliseconds,
// UTC) before the write; explicit timestamps are preserved.
//
// Durability: the record is written (page cache) before Append returns; with
// sync=true the file is fsynced before returning. On a write error the
// offset is not consumed and any partial bytes are truncated away; on a sync
// error the offset IS consumed (the record is in the file) but the caller
// must not treat it as flushed.
func (l *Log) Append(rec record.Record, sync bool) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if rec.Timestamp == 0 {
		rec.Timestamp = time.Now().UnixMilli()
	}
	buf, err := rec.Encode(l.maxSize)
	if err != nil {
		return 0, err
	}
	writeAt := l.end
	if _, err := l.file.WriteAt(buf, writeAt); err != nil {
		// Best effort: remove any partial bytes so the file stays a
		// valid prefix; the failed record will be rewritten on retry.
		_ = l.file.Truncate(writeAt)
		return 0, fmt.Errorf("logengine: append at position %d: %w", writeAt, err)
	}
	if sync {
		if err := l.file.Sync(); err != nil {
			return uint64(len(l.positions)), fmt.Errorf("logengine: sync after append: %w", err)
		}
	}
	l.positions = append(l.positions, writeAt)
	l.end = writeAt + int64(len(buf))
	return uint64(len(l.positions) - 1), nil
}

// ReadFrom returns records starting at offset, up to maxBytes of consumed
// encoded size, together with the offset just past the last returned record.
// At least one record is returned when one is available, even if it alone
// exceeds maxBytes. Reading exactly at NextOffset returns an empty batch
// (the at-end case); an offset beyond that is ErrOffsetOutOfRange.
func (l *Log) ReadFrom(offset uint64, maxBytes int) ([]record.Record, uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, 0, ErrClosed
	}
	if maxBytes <= 0 {
		return nil, 0, ErrInvalidMaxBytes
	}
	next := uint64(len(l.positions))
	if offset > next {
		return nil, 0, fmt.Errorf("%w: offset %d is beyond next offset %d", ErrOffsetOutOfRange, offset, next)
	}
	if offset == next {
		return nil, next, nil
	}
	start := l.positions[offset]
	// Read only the completed region, capped so a small maxBytes cannot
	// force a whole-log allocation: enough for maxBytes plus one more
	// maximum-size record.
	alloc := l.end - start
	if budget := int64(maxBytes) + int64(l.maxSize); budget < alloc {
		alloc = budget
	}
	buf := make([]byte, alloc)
	if _, err := l.file.ReadAt(buf, start); err != nil {
		// [start, end) was fully written under the write lock, so a
		// short read here is an I/O error, not a torn record.
		return nil, 0, fmt.Errorf("logengine: reading from position %d: %w", start, err)
	}
	var (
		recs     []record.Record
		consumed int
		count    uint64
	)
	for offset+count < next {
		rec, n, err := record.Decode(buf[consumed:], l.maxSize)
		if err != nil {
			return nil, 0, fmt.Errorf("logengine: decoding record at offset %d: %w", offset+count, err)
		}
		if count >= 1 && consumed+n > maxBytes {
			// Including this record would exceed the batch limit; it is
			// returned to the next read (at-least-one already met).
			break
		}
		recs = append(recs, rec)
		consumed += n
		count++
	}
	return recs, offset + count, nil
}

// Scan calls fn for every record in offset order, decoding the whole file.
// It returns the first error from fn or from decoding.
func (l *Log) Scan(fn func(rec record.Record, offset uint64) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return ErrClosed
	}
	r := io.NewSectionReader(l.file, 0, l.end)
	for offset := uint64(0); ; offset++ {
		rec, _, err := record.DecodeFrom(r, l.maxSize)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("logengine: scan failed at offset %d: %w", offset, err)
		}
		if err := fn(rec, offset); err != nil {
			return err
		}
	}
}

// NextOffset returns the offset the next appended record will receive.
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.positions))
}

// LastOffset returns the offset of the most recent record, or false for an
// empty log.
func (l *Log) LastOffset() (uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.positions) == 0 {
		return 0, false
	}
	return uint64(len(l.positions) - 1), true
}

// TruncatedOnRecover returns the number of bytes Open discarded while
// recovering to the longest valid prefix (0 when the file was clean).
func (l *Log) TruncatedOnRecover() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.truncated
}

// Close closes the underlying file. It performs no implicit fsync:
// durability is decided per append via the sync flag.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.closed = true
	return l.file.Close()
}
