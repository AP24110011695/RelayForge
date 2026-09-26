// Package logengine implements RelayForge's append-only log: a set of
// segment files with sparse indexes, durability levels, crash recovery, and
// retention.
//
// A log is a directory of segments named <base>.log / <base>.index (20-digit
// zero-padded base offset). All segments except the last are sealed and
// immutable; appends go to the active (last) segment, which rolls when it
// would exceed MaxSegmentBytes. Offsets are per-log, 0-based, contiguous
// sequence numbers — never file positions. Indexes are derived state: they
// are validated at Open, rebuilt from the log when invalid, and never
// trusted over record validation.
//
// Concurrency model: one RWMutex; appends take the write lock and complete
// their write before releasing it, so readers (read lock) only ever see
// fully written records. All file access is position-based (ReadAt/WriteAt).
package logengine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"relayforge/internal/record"
)

// Defaults for Options fields left at zero.
const (
	DefaultMaxRecordSize = 4 << 20 // 4 MiB per record (header + key + value)
)

// Durability is the durability ladder promised by an append.
type Durability int

const (
	// DurabilityNone makes no durability promise. In this synchronous
	// engine API the bytes are still written before Append returns (a
	// caller could not observe otherwise); the mode exists so callers
	// can express intent and so the broker's async path (Phase 6) has a
	// real distinction to implement.
	DurabilityNone Durability = iota
	// DurabilityWritten promises the record has reached the page cache
	// when Append returns: survives a process crash, not power loss.
	DurabilityWritten
	// DurabilityFlushed promises fsync completed before Append returns:
	// intended to survive power loss subject to the documented
	// filesystem assumptions (docs/GUARANTEES.md).
	DurabilityFlushed
)

// Options configures Open. Zero fields take defaults.
type Options struct {
	// MaxRecordSize caps the full encoded size of one record.
	// 0 selects DefaultMaxRecordSize.
	MaxRecordSize int
	// MaxSegmentBytes rolls the active segment once it would exceed this
	// size. 0 selects DefaultMaxSegmentBytes. Must be >= MaxRecordSize.
	MaxSegmentBytes int64
	// IndexIntervalBytes writes a sparse index entry roughly every this
	// many bytes of the active segment. 0 selects
	// DefaultIndexIntervalBytes.
	IndexIntervalBytes int64
	// Retention, when set, is applied at Open and via ApplyRetention.
	Retention RetentionPolicy
}

// RetentionPolicy controls segment-granularity retention. Zero fields
// disable the respective limit. The active segment is never deleted.
type RetentionPolicy struct {
	// MaxAge drops sealed segments whose log file was last modified
	// longer ago than this.
	MaxAge time.Duration
	// MaxBytes drops the oldest sealed segments until the total size of
	// all segments is at or below this. Best-effort: the active segment
	// is never deleted, so the floor is MaxSegmentBytes.
	MaxBytes int64
}

func (p RetentionPolicy) active() bool { return p.MaxAge > 0 || p.MaxBytes > 0 }

// Errors returned by log operations.
var (
	// ErrOffsetOutOfRange means the requested offset is not in the log:
	// either beyond the end (>= NextOffset) or below the earliest
	// retained offset (deleted by retention).
	ErrOffsetOutOfRange = errors.New("logengine: offset out of range")
	// ErrInvalidMaxBytes means a ReadFrom batch limit is not positive.
	ErrInvalidMaxBytes = errors.New("logengine: maxBytes must be positive")
	// ErrClosed is returned by operations after Close, and by a second
	// Close.
	ErrClosed = errors.New("logengine: log is closed")
	// ErrOffsetHole is reported by Scan when segments do not chain
	// (records are missing between two segment bases).
	ErrOffsetHole = errors.New("logengine: offset hole in segment sequence")
)

// Log is an append-only log backed by a segment set. It is safe for
// concurrent use: one writer at a time (serialized internally), many
// concurrent readers.
type Log struct {
	mu              sync.RWMutex
	dir             string
	maxRecordSize   int
	maxSegmentBytes int64
	indexInterval   int64
	retention       RetentionPolicy
	segments        []*segment
	wrap            func(base uint64, f *os.File) segmentFile
	next            uint64
	truncated       int64 // bytes discarded by recovery at Open
	degraded        bool  // set after a failed fsync; cleared on the next successful one
	closed          bool
}

// Open opens (or creates) the log directory and recovers it:
//
//   - sealed segments are trusted via cheap consistency checks on their
//     indexes; an invalid index is rebuilt by re-scanning that segment's
//     log, and a log that does not decode makes Open fail (sealed history
//     is never auto-truncated);
//   - the active segment is fully scanned and truncated from the first
//     invalid record onward (torn tail from a crash, or corruption);
//   - the active segment's index is rewritten to match the scan;
//   - when a retention policy is configured it is applied once.
func Open(dir string, opts Options) (*Log, error) {
	return openLog(dir, opts, nil)
}

// openLog is Open with an optional file wrapper, the seam used by the
// fault-injection tests.
func openLog(dir string, opts Options, wrap func(base uint64, f *os.File) segmentFile) (*Log, error) {
	maxRecordSize := opts.MaxRecordSize
	if maxRecordSize == 0 {
		maxRecordSize = DefaultMaxRecordSize
	}
	if maxRecordSize < 0 {
		return nil, fmt.Errorf("logengine: MaxRecordSize %d must not be negative", maxRecordSize)
	}
	maxSegmentBytes := opts.MaxSegmentBytes
	if maxSegmentBytes == 0 {
		maxSegmentBytes = DefaultMaxSegmentBytes
	}
	if maxSegmentBytes < int64(maxRecordSize) {
		return nil, fmt.Errorf("logengine: MaxSegmentBytes %d must be >= MaxRecordSize %d",
			maxSegmentBytes, maxRecordSize)
	}
	indexInterval := opts.IndexIntervalBytes
	if indexInterval == 0 {
		indexInterval = DefaultIndexIntervalBytes
	}
	if indexInterval < 0 {
		return nil, fmt.Errorf("logengine: IndexIntervalBytes %d must not be negative", indexInterval)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{
		dir:             dir,
		maxRecordSize:   maxRecordSize,
		maxSegmentBytes: maxSegmentBytes,
		indexInterval:   indexInterval,
		retention:       opts.Retention,
		wrap:            wrap,
	}
	if err := l.loadSegments(wrap); err != nil {
		l.closeAll()
		return nil, err
	}
	if l.retention.active() {
		if err := l.ApplyRetention(time.Now()); err != nil {
			l.closeAll()
			return nil, fmt.Errorf("logengine: initial retention: %w", err)
		}
	}
	return l, nil
}

// loadSegments discovers, validates, and recovers all segments.
func (l *Log) loadSegments(wrap func(base uint64, f *os.File) segmentFile) error {
	dirEntries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	var bases []uint64
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		if base, ok := segmentBaseFromName(de.Name()); ok {
			bases = append(bases, base)
			continue
		}
		// Stale temp files from an interrupted index rewrite.
		if len(de.Name()) > len(tempSuffix) && de.Name()[len(de.Name())-len(tempSuffix):] == tempSuffix {
			if err := os.Remove(filepath.Join(l.dir, de.Name())); err != nil {
				return err
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	for i := 1; i < len(bases); i++ {
		if bases[i] == bases[i-1] {
			return fmt.Errorf("logengine: duplicate segment base %d", bases[i])
		}
	}
	if len(bases) == 0 {
		if _, err := openSegment(l.dir, 0, true, wrap); err != nil {
			return err
		}
		if err := syncDir(l.dir); err != nil {
			return err
		}
		bases = []uint64{0}
	}
	for i, base := range bases {
		seg, err := openSegment(l.dir, base, false, wrap)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
		if i < len(bases)-1 {
			// Sealed: cheap checks; rebuild the index when invalid.
			if err := seg.loadIndex(); err != nil {
				if err := seg.rebuildIndex(l.dir, l.maxRecordSize); err != nil {
					seg.closeFiles()
					return fmt.Errorf("logengine: sealed segment %d is unusable: %w", base, err)
				}
			}
			seg.sealed = true
			seg.idxFile.Close()
			seg.idxFile = nil
		}
	}
	// Active segment: full validating scan, then truncate the invalid tail.
	active := l.segments[len(l.segments)-1]
	count, entries, truncated, err := scanActive(active, l.maxRecordSize)
	if err != nil {
		return err
	}
	l.truncated = truncated
	if truncated > 0 {
		if err := active.logFile.Truncate(active.size - truncated); err != nil {
			return err
		}
		active.size -= truncated
	}
	// The active index is derived from the scan; rewrite when it differs.
	if !indexMatches(active.idxFile, entries) {
		if err := writeIndexFile(l.dir, active.base, entries); err != nil {
			return err
		}
	}
	active.entries = entries
	active.idxLen = int64(len(entries)) * indexEntrySize
	if len(entries) > 0 {
		active.bytesSinceIndex = active.size - int64(entries[len(entries)-1].position)
	}
	l.next = active.base + count
	return nil
}

// scanActive decodes the active segment from its start and returns the
// record count of the valid prefix, the index entries implied by it, and
// the byte count discarded from the first invalid record onward.
func scanActive(seg *segment, maxSize int) (count uint64, entries []indexEntry, truncated int64, err error) {
	var since int64
	var pos int64
	r := io.NewSectionReader(seg.logFile, 0, seg.size)
	for {
		_, n, derr := record.DecodeFrom(r, maxSize)
		if errors.Is(derr, io.EOF) {
			return count, entries, 0, nil
		}
		if isCorruption(derr) {
			return count, entries, seg.size - pos, nil
		}
		if derr != nil {
			// A transient I/O error must not cause truncation.
			return 0, nil, 0, fmt.Errorf("logengine: scanning active segment %d at position %d: %w", seg.base, pos, derr)
		}
		if count == 0 || since >= seg.indexInterval {
			entries = append(entries, indexEntry{offset: seg.base + count, position: uint64(pos)})
		}
		pos += int64(n)
		since += int64(n)
		count++
	}
}

// indexMatches reports whether the on-disk index equals the expected
// entries.
func indexMatches(f segmentFile, entries []indexEntry) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	if info.Size() != int64(len(entries))*indexEntrySize {
		return false
	}
	buf := make([]byte, info.Size())
	if _, err := f.ReadAt(buf, 0); err != nil {
		return false
	}
	for i, e := range entries {
		if binary.LittleEndian.Uint64(buf[i*indexEntrySize:]) != e.offset ||
			binary.LittleEndian.Uint64(buf[i*indexEntrySize+8:]) != e.position {
			return false
		}
	}
	return true
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

// Append validates and writes one record to the active segment, rolling the
// segment first when the record would overflow it, and returns the assigned
// offset. A zero Timestamp is stamped with the current wall clock
// (milliseconds, UTC).
//
// Durability: the record is written (page cache) before Append returns
// regardless of dur; DurabilityFlushed additionally fsyncs the active
// segment before returning. On a write error the offset is not consumed and
// any partial bytes are truncated away. On a sync error the offset IS
// consumed (the bytes are in the file) but the caller must not treat the
// record as flushed; the log enters the degraded state until the next
// successful fsync.
func (l *Log) Append(rec record.Record, dur Durability) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if rec.Timestamp == 0 {
		rec.Timestamp = time.Now().UnixMilli()
	}
	buf, err := rec.Encode(l.maxRecordSize)
	if err != nil {
		return 0, err
	}
	active := l.segments[len(l.segments)-1]
	if active.size > 0 && active.size+int64(len(buf)) > l.maxSegmentBytes {
		if err := l.rollLocked(); err != nil {
			return 0, err
		}
		active = l.segments[len(l.segments)-1]
	}
	position := active.size
	n, err := active.logFile.WriteAt(buf, position)
	if err == nil && n != len(buf) {
		err = fmt.Errorf("%w: wrote %d of %d bytes", io.ErrShortWrite, n, len(buf))
	}
	if err != nil {
		// Best effort: remove any partial bytes so the file stays a
		// valid prefix; the failed record will be rewritten on retry.
		_ = active.logFile.Truncate(position)
		return 0, fmt.Errorf("logengine: append at position %d: %w", position, err)
	}
	// The bytes are in the file: advance all state before the durability
	// step, so even a failed fsync leaves a consistent log that owns the
	// offset (the caller must simply not treat the record as flushed).
	active.maybeIndex(l.next, position)
	active.bytesSinceIndex += int64(len(buf))
	active.size += int64(len(buf))
	offset := l.next
	l.next++
	if dur == DurabilityFlushed {
		if err := active.logFile.Sync(); err != nil {
			l.degraded = true
			return offset, fmt.Errorf("logengine: fsync after append: %w", err)
		}
		l.degraded = false
	}
	return offset, nil
}

// rollLocked seals the active segment and starts a new one at the next
// offset. The directory is fsynced so the new segment's namespace entry
// survives crashes.
func (l *Log) rollLocked() error {
	old := l.segments[len(l.segments)-1]
	old.sealed = true
	old.idxFile.Close()
	old.idxFile = nil
	seg, err := openSegment(l.dir, l.next, true, l.wrap)
	if err != nil {
		return fmt.Errorf("logengine: rolling segment at offset %d: %w", l.next, err)
	}
	l.segments = append(l.segments, seg)
	return syncDir(l.dir)
}

// ReadFrom returns records starting at offset, up to maxBytes of returned
// encoded size, together with the offset just past the last returned
// record. At least one record is returned when one is available, even if it
// alone exceeds maxBytes. Reads do not cross segment boundaries: a batch
// ends at the segment's end and the caller continues from the returned
// offset. Reading exactly at NextOffset returns an empty batch (the at-end
// case); an offset beyond that, or one deleted by retention, is
// ErrOffsetOutOfRange.
func (l *Log) ReadFrom(offset uint64, maxBytes int) ([]record.Record, uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, 0, ErrClosed
	}
	if maxBytes <= 0 {
		return nil, 0, ErrInvalidMaxBytes
	}
	if offset >= l.next {
		if offset == l.next {
			return nil, l.next, nil
		}
		return nil, 0, fmt.Errorf("%w: offset %d is beyond next offset %d", ErrOffsetOutOfRange, offset, l.next)
	}
	i := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].base > offset }) - 1
	if i < 0 {
		return nil, 0, fmt.Errorf("%w: offset %d is below the earliest retained offset %d",
			ErrOffsetOutOfRange, offset, l.segments[0].base)
	}
	seg := l.segments[i]
	segEnd := l.next
	if i < len(l.segments)-1 {
		segEnd = l.segments[i+1].base
	}
	entry, ok := seg.lookupEntry(offset)
	if !ok {
		return nil, 0, fmt.Errorf("logengine: no index entry at or before offset %d in segment %d", offset, seg.base)
	}
	pos := int64(entry.position)
	// Bounded read: the skip region to the requested record is < interval
	// + maxSize, and the collected region is at most one maxSize record
	// plus maxBytes.
	avail := seg.size - pos
	budget := int64(maxBytes) + 2*int64(l.maxRecordSize) + l.indexInterval
	alloc := avail
	if budget < alloc {
		alloc = budget
	}
	buf := make([]byte, alloc)
	if _, err := seg.logFile.ReadAt(buf, pos); err != nil {
		// Fully written region under the write lock, so a short read is
		// an I/O error, not a torn record.
		return nil, 0, fmt.Errorf("logengine: reading segment %d at position %d: %w", seg.base, pos, err)
	}
	var (
		recs      []record.Record
		consumed  int // bytes of buf consumed (skipped + collected)
		collected int // bytes of collected (returned) records
		count     uint64
		running   = entry.offset
	)
	for running < segEnd {
		rec, n, err := record.Decode(buf[consumed:], l.maxRecordSize)
		if err != nil {
			return nil, 0, fmt.Errorf("logengine: decoding segment %d offset %d: %w", seg.base, running, err)
		}
		if running < offset {
			// Skip forward to the requested offset.
			consumed += n
			running++
			continue
		}
		if count >= 1 && collected+n > maxBytes {
			// This record would exceed the batch limit; it is left
			// for the next read (at-least-one already met).
			break
		}
		recs = append(recs, rec)
		consumed += n
		collected += n
		count++
		running++
	}
	return recs, offset + count, nil
}

// Scan calls fn for every record in offset order, fully decoding and
// validating every segment, and verifies offset continuity across segments
// (a hole in the segment sequence is an error). It is the deep-verify
// utility; it returns the first error from fn or from decoding.
func (l *Log) Scan(fn func(rec record.Record, offset uint64) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return ErrClosed
	}
	for i, seg := range l.segments {
		expected := seg.base
		r := io.NewSectionReader(seg.logFile, 0, seg.size)
		for {
			rec, _, err := record.DecodeFrom(r, l.maxRecordSize)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("logengine: scan failed in segment %d: %w", seg.base, err)
			}
			if err := fn(rec, expected); err != nil {
				return err
			}
			expected++
		}
		if i < len(l.segments)-1 && expected != l.segments[i+1].base {
			return fmt.Errorf("%w: segment %d holds %d records but the next segment base is %d",
				ErrOffsetHole, seg.base, expected-seg.base, l.segments[i+1].base)
		}
	}
	return nil
}

// NextOffset returns the offset the next appended record will receive.
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.next
}

// LastOffset returns the offset of the most recent record, or false for an
// empty log.
func (l *Log) LastOffset() (uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.next == 0 {
		return 0, false
	}
	return l.next - 1, true
}

// TruncatedOnRecover returns the number of bytes Open discarded while
// recovering the active segment to the longest valid prefix (0 when the
// file was clean).
func (l *Log) TruncatedOnRecover() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.truncated
}

// Degraded reports whether the last fsync attempt failed. While degraded,
// DurabilityFlushed appends keep retrying the fsync and keep failing until
// it succeeds; written-mode appends are unaffected.
func (l *Log) Degraded() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.degraded
}

// Close closes all segment files. It performs no implicit fsync: durability
// is decided per append via the durability argument.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.closed = true
	l.closeAll()
	return nil
}

func (l *Log) closeAll() {
	for _, seg := range l.segments {
		seg.closeFiles()
	}
}
