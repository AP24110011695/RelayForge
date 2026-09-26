package logengine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"relayforge/internal/record"
)

// segmentFile is the file-level seam the engine operates on. *os.File
// satisfies it directly; the fault-injection tests substitute wrappers that
// inject ENOSPC, short writes, sync errors, and sync latency.
type segmentFile interface {
	WriteAt(p []byte, off int64) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	Sync() error
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
	Close() error
}

const (
	indexEntrySize            = 16 // uint64 offset + uint64 position, little-endian
	logSuffix                 = ".log"
	indexSuffix               = ".index"
	tempSuffix                = ".tmp"
	baseDigits                = 20
	DefaultMaxSegmentBytes    = int64(64 << 20) // 64 MiB
	DefaultIndexIntervalBytes = int64(4096)
)

type indexEntry struct {
	offset   uint64
	position uint64
}

// segment is one append-only log file plus its derived sparse index.
// Sealed segments are immutable: only the last segment is appended to.
type segment struct {
	base            uint64
	logFile         segmentFile
	idxFile         segmentFile // nil once sealed
	size            int64       // log file size in bytes
	indexInterval   int64
	entries         []indexEntry
	idxLen          int64 // valid index bytes on disk
	bytesSinceIndex int64
	sealed          bool
}

func segmentLogPath(dir string, base uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%0*d%s", baseDigits, base, logSuffix))
}

func segmentIndexPath(dir string, base uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%0*d%s", baseDigits, base, indexSuffix))
}

func segmentBaseFromName(name string) (uint64, bool) {
	if len(name) != baseDigits+len(logSuffix) || name[baseDigits:] != logSuffix {
		return 0, false
	}
	base, err := strconv.ParseUint(name[:baseDigits], 10, 64)
	if err != nil {
		return 0, false
	}
	return base, true
}

// openSegment opens (creating when asked) the log and index files for a
// segment, wrapping them through wrap when provided. The index is always
// opened with O_CREATE: a missing index is rebuildable derived state, not
// an error.
func openSegment(dir string, base uint64, create bool, wrap func(base uint64, f *os.File) segmentFile) (*segment, error) {
	logFlags := os.O_RDWR
	if create {
		logFlags |= os.O_CREATE
	}
	logF, err := os.OpenFile(segmentLogPath(dir, base), logFlags, 0o644)
	if err != nil {
		return nil, err
	}
	idxF, err := os.OpenFile(segmentIndexPath(dir, base), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		logF.Close()
		return nil, err
	}
	seg := &segment{base: base, logFile: logF, idxFile: idxF}
	if wrap != nil {
		seg.logFile = wrap(base, logF)
		seg.idxFile = wrap(base, idxF)
	}
	if info, err := seg.logFile.Stat(); err != nil {
		seg.closeFiles()
		return nil, err
	} else {
		seg.size = info.Size()
	}
	return seg, nil
}

// loadIndex reads and validates the on-disk index. The index is derived
// state: a failing validation is reported so the caller can rebuild it from
// the log, never served to readers.
func (s *segment) loadIndex() error {
	info, err := s.idxFile.Stat()
	if err != nil {
		return err
	}
	if info.Size()%indexEntrySize != 0 {
		return fmt.Errorf("index size %d is not a multiple of %d", info.Size(), indexEntrySize)
	}
	buf := make([]byte, info.Size())
	if _, err := s.idxFile.ReadAt(buf, 0); err != nil {
		return err
	}
	entries := make([]indexEntry, 0, len(buf)/indexEntrySize)
	for off := 0; off < len(buf); off += indexEntrySize {
		entries = append(entries, indexEntry{
			offset:   binary.LittleEndian.Uint64(buf[off:]),
			position: binary.LittleEndian.Uint64(buf[off+8:]),
		})
	}
	if err := s.validateEntries(entries); err != nil {
		return err
	}
	s.entries = entries
	s.idxLen = info.Size()
	return nil
}

func (s *segment) validateEntries(entries []indexEntry) error {
	if len(entries) == 0 {
		return errors.New("index has no entries")
	}
	if entries[0].offset != s.base || entries[0].position != 0 {
		return fmt.Errorf("first index entry (%d, %d) does not anchor the segment base %d",
			entries[0].offset, entries[0].position, s.base)
	}
	for i, e := range entries {
		if i > 0 {
			if e.offset <= entries[i-1].offset {
				return fmt.Errorf("index offsets not increasing at entry %d", i)
			}
			if e.position <= entries[i-1].position {
				return fmt.Errorf("index positions not increasing at entry %d", i)
			}
		}
		if e.position >= uint64(s.size) {
			return fmt.Errorf("index position %d beyond log size %d", e.position, s.size)
		}
	}
	return nil
}

// rebuildIndex reconstructs the index by fully scanning and validating the
// log file. It fails if the log itself does not decode.
func (s *segment) rebuildIndex(dir string, maxSize int) error {
	var entries []indexEntry
	var since int64
	var pos int64
	count := uint64(0)
	r := io.NewSectionReader(s.logFile, 0, s.size)
	for {
		_, n, err := record.DecodeFrom(r, maxSize)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("segment base %d: log does not decode at position %d: %w", s.base, pos, err)
		}
		if count == 0 || since >= s.indexInterval {
			entries = append(entries, indexEntry{offset: s.base + count, position: uint64(pos)})
		}
		pos += int64(n)
		since += int64(n)
		count++
	}
	if err := writeIndexFile(dir, s.base, entries); err != nil {
		return err
	}
	s.entries = entries
	s.idxLen = int64(len(entries)) * indexEntrySize
	return nil
}

// writeIndexFile atomically replaces a segment's index file (temp + rename +
// directory fsync) — the namespace must survive crashes for the rebuild to
// stick, though a lost index is always rebuildable.
func writeIndexFile(dir string, base uint64, entries []indexEntry) error {
	buf := make([]byte, len(entries)*indexEntrySize)
	for i, e := range entries {
		binary.LittleEndian.PutUint64(buf[i*indexEntrySize:], e.offset)
		binary.LittleEndian.PutUint64(buf[i*indexEntrySize+8:], e.position)
	}
	tmp := segmentIndexPath(dir, base) + tempSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, segmentIndexPath(dir, base)); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// maybeIndex appends an index entry for the record at (offset, position)
// when the byte interval since the last entry has been reached. Index write
// errors are deliberately ignored: the index is derived state, and a short
// index is detected and rebuilt at the next Open.
func (s *segment) maybeIndex(offset uint64, position int64) {
	if len(s.entries) == 0 || s.bytesSinceIndex >= s.indexInterval {
		s.entries = append(s.entries, indexEntry{offset: offset, position: uint64(position)})
		var e [indexEntrySize]byte
		binary.LittleEndian.PutUint64(e[:8], offset)
		binary.LittleEndian.PutUint64(e[8:], uint64(position))
		_, _ = s.idxFile.WriteAt(e[:], s.idxLen)
		s.idxLen += indexEntrySize
		s.bytesSinceIndex = 0
	}
}

// lookup returns the file position of the record with the given offset,
// found via binary search over the sparse index. The caller scans forward
// from that position to the exact record.
func (s *segment) lookup(offset uint64) (int64, bool) {
	i := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].offset > offset })
	if i == 0 {
		return 0, false
	}
	return int64(s.entries[i-1].position), true
}

// lookupEntryOffset returns the offset of the index entry a lookup landed
// on, so the reader can count records from a known offset.
func (s *segment) lookupEntry(offset uint64) (indexEntry, bool) {
	i := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].offset > offset })
	if i == 0 {
		return indexEntry{}, false
	}
	return s.entries[i-1], true
}

func (s *segment) closeFiles() {
	if s.logFile != nil {
		s.logFile.Close()
		s.logFile = nil
	}
	if s.idxFile != nil {
		s.idxFile.Close()
		s.idxFile = nil
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
