// Package record implements RelayForge's on-disk record format v1.
//
// A record is a 4-byte little-endian frame length followed by exactly that
// many bytes: a CRC32C (Castagnoli) over everything after the CRC field, a
// version byte, a reserved attributes byte, an 8-byte timestamp, and the
// key/value lengths and bytes. See docs/RECORD_FORMAT.md for the full
// contract. The length-prefix framing discipline is shared with
// internal/framing; this package is self-contained by design.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// Version1 is the only defined record version; decoders reject others.
const Version1 byte = 1

// HeaderSize is the fixed part of every record: frame length (4) + CRC32C
// (4) + version (1) + attributes (1) + timestamp (8) + key length (4) +
// value length (4).
const HeaderSize = 26

// MinFrameLength is the smallest valid frame length: the fixed fields after
// the length prefix (CRC through value length) with empty key and value.
const MinFrameLength = HeaderSize - 4

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors returned when decoding or encoding. Truncation is reported as
// io.EOF (clean record boundary) or io.ErrUnexpectedEOF (mid-record); other
// reader/writer errors surface unchanged.
var (
	// ErrChecksumMismatch means the stored CRC32C does not match the one
	// computed over the record body.
	ErrChecksumMismatch = errors.New("record: CRC32C mismatch")
	// ErrUnsupportedVersion means the version byte is not Version1.
	ErrUnsupportedVersion = errors.New("record: unsupported version")
	// ErrReservedAttributes means the attributes byte is nonzero, which
	// v1 forbids.
	ErrReservedAttributes = errors.New("record: reserved attributes bits set")
	// ErrMalformed means a structural violation: frame length below the
	// minimum, or key/value lengths inconsistent with the frame length.
	ErrMalformed = errors.New("record: malformed record")
	// ErrRecordTooLarge means a record exceeds the configured size limit.
	ErrRecordTooLarge = errors.New("record: record exceeds size limit")
	// ErrInvalidLimit means a size limit argument is not positive.
	ErrInvalidLimit = errors.New("record: size limit must be positive")
)

// Record is a single log record. Nil Key/Value are treated as empty.
type Record struct {
	Key        []byte
	Value      []byte
	Timestamp  int64 // milliseconds since the Unix epoch, UTC
	Attributes byte  // reserved; must be 0 in v1
}

// Size validates the record against maxSize and returns its full encoded
// size: HeaderSize + len(Key) + len(Value).
func (r Record) Size(maxSize int) (int, error) {
	if maxSize <= 0 {
		return 0, ErrInvalidLimit
	}
	if r.Attributes != 0 {
		return 0, ErrReservedAttributes
	}
	total := HeaderSize + len(r.Key) + len(r.Value)
	if total > maxSize {
		return 0, fmt.Errorf("%w: %d bytes > limit %d", ErrRecordTooLarge, total, maxSize)
	}
	// The frame length field is 4 bytes wide; frameLen = total - 4.
	if uint64(total)-4 > math.MaxUint32 {
		return 0, fmt.Errorf("%w: %d bytes exceed the encodable maximum", ErrRecordTooLarge, total)
	}
	return total, nil
}

// Encode returns the fully encoded record in a freshly allocated buffer.
func (r Record) Encode(maxSize int) ([]byte, error) {
	return r.AppendEncoded(nil, maxSize)
}

// AppendEncoded appends the encoded record to dst (growing it as needed)
// and returns the slice. Callers can reuse dst across calls to avoid
// per-record allocations.
func (r Record) AppendEncoded(dst []byte, maxSize int) ([]byte, error) {
	total, err := r.Size(maxSize)
	if err != nil {
		return nil, err
	}
	keyLen := len(r.Key)
	valLen := len(r.Value)

	buf := dst
	if cap(buf) < total {
		buf = make([]byte, total)
	}
	buf = buf[:total]
	binary.LittleEndian.PutUint32(buf[0:4], uint32(total-4)) // frame length
	body := buf[8:]                                          // version through value: the CRC-protected region
	body[0] = Version1
	body[1] = r.Attributes
	binary.LittleEndian.PutUint64(body[2:10], uint64(r.Timestamp))
	binary.LittleEndian.PutUint32(body[10:14], uint32(keyLen))
	copy(body[14:14+keyLen], r.Key)
	binary.LittleEndian.PutUint32(body[14+keyLen:18+keyLen], uint32(valLen))
	copy(body[18+keyLen:], r.Value)
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(body, castagnoli))
	return buf, nil
}

// EncodeTo writes the encoded record to w as a single Write of the fully
// assembled buffer. Per the io.Writer contract, a short write surfaces as an
// error; callers writing to files therefore get complete-or-error writes.
func (r Record) EncodeTo(w io.Writer, maxSize int) error {
	buf, err := r.Encode(maxSize)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// DecodeFrom reads exactly one record from r and returns the number of bytes
// consumed (0 on error). It returns io.EOF when the stream ends cleanly at a
// record boundary (zero bytes read) and io.ErrUnexpectedEOF when it ends
// mid-record.
func DecodeFrom(r io.Reader, maxSize int) (Record, int, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Record{}, 0, err
	}
	frameLen := binary.LittleEndian.Uint32(lenBuf[:])
	if err := checkFrameLen(frameLen, maxSize); err != nil {
		return Record{}, 0, err
	}
	frame := make([]byte, frameLen)
	if _, err := io.ReadFull(r, frame); err != nil {
		if errors.Is(err, io.EOF) {
			// The length prefix was consumed, so the stream cannot end
			// cleanly here: this is a mid-record truncation.
			err = io.ErrUnexpectedEOF
		}
		return Record{}, 0, err
	}
	rec, err := parse(frame)
	if err != nil {
		return Record{}, 0, err
	}
	return rec, int(frameLen) + 4, nil
}

// Decode parses one record from data and returns the number of bytes
// consumed (0 on error). Trailing bytes beyond one record are not consumed.
// The decoded Record aliases data (zero-copy): callers that retain it must
// copy.
func Decode(data []byte, maxSize int) (Record, int, error) {
	if len(data) == 0 {
		return Record{}, 0, io.EOF
	}
	if len(data) < 4 {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	frameLen := binary.LittleEndian.Uint32(data[:4])
	if err := checkFrameLen(frameLen, maxSize); err != nil {
		return Record{}, 0, err
	}
	if uint64(frameLen)+4 > uint64(len(data)) {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	rec, err := parse(data[4 : 4+frameLen])
	if err != nil {
		return Record{}, 0, err
	}
	return rec, int(frameLen) + 4, nil
}

// checkFrameLen validates a declared frame length against the limit before
// any allocation happens.
func checkFrameLen(frameLen uint32, maxSize int) error {
	if maxSize <= 0 {
		return ErrInvalidLimit
	}
	if int(frameLen) < MinFrameLength {
		return fmt.Errorf("%w: frame length %d below minimum %d", ErrMalformed, frameLen, MinFrameLength)
	}
	if int(frameLen) > maxSize-4 {
		return fmt.Errorf("%w: frame length %d > limit %d", ErrRecordTooLarge, frameLen, maxSize-4)
	}
	return nil
}

// parse validates and interprets one frame: the bytes after the length
// prefix, starting at the CRC field. The returned Record aliases frame.
func parse(frame []byte) (Record, error) {
	if len(frame) < MinFrameLength {
		return Record{}, fmt.Errorf("%w: frame %d bytes below minimum %d", ErrMalformed, len(frame), MinFrameLength)
	}
	crc := binary.LittleEndian.Uint32(frame[0:4])
	body := frame[4:] // version through value: the CRC-protected region
	if sum := crc32.Checksum(body, castagnoli); sum != crc {
		return Record{}, fmt.Errorf("%w: stored %#08x, computed %#08x", ErrChecksumMismatch, crc, sum)
	}
	if body[0] != Version1 {
		return Record{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, body[0])
	}
	if body[1] != 0 {
		return Record{}, ErrReservedAttributes
	}
	timestamp := int64(binary.LittleEndian.Uint64(body[2:10]))
	keyLen := binary.LittleEndian.Uint32(body[10:14])
	if uint64(keyLen) > uint64(len(body)-18) {
		return Record{}, fmt.Errorf("%w: key length %d exceeds frame", ErrMalformed, keyLen)
	}
	valOff := 14 + int(keyLen)
	valLen := binary.LittleEndian.Uint32(body[valOff : valOff+4])
	if int(valLen) != len(body)-valOff-4 {
		return Record{}, fmt.Errorf("%w: value length %d does not match frame", ErrMalformed, valLen)
	}
	return Record{
		Key:       body[14 : 14+int(keyLen)],
		Value:     body[valOff+4:],
		Timestamp: timestamp,
	}, nil
}
