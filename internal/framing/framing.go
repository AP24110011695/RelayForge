// Package framing implements length-prefix framing for byte streams.
//
// A frame on the wire is a 4-byte little-endian unsigned length prefix
// followed by exactly that many payload bytes. The prefix counts only the
// payload, not itself. Payloads are opaque: no CRC, no version, no escaping.
// See docs/FRAMING.md for the full contract.
package framing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	// headerSize is the size of the length prefix.
	headerSize = 4
	// maxEncodeLength is the largest payload a frame can encode: the
	// length prefix is 4 bytes wide.
	maxEncodeLength uint64 = math.MaxUint32
)

// Errors returned by ReadFrame and WriteFrame. Transport errors (deadlines,
// connection resets, ...) surface unchanged so callers can classify them
// with errors.Is.
var (
	// ErrFrameTooLarge means a frame exceeds the applicable size limit:
	// either a payload passed to WriteFrame, or a length declared by an
	// incoming frame's header. After ReadFrame returns it, the stream is
	// misaligned and must be abandoned (the oversized payload is not
	// consumed).
	ErrFrameTooLarge = errors.New("framing: frame exceeds size limit")

	// ErrInvalidLimit is returned when a size limit is not positive.
	ErrInvalidLimit = errors.New("framing: size limit must be positive")
)

// WriteFrame writes payload to w as one frame: a 4-byte little-endian length
// prefix followed by the payload. It guarantees complete writes even if w
// performs short writes. Wrap w in a bufio.Writer when write coalescing
// matters; this package does one Write call per piece (header, payload).
func WriteFrame(w io.Writer, payload []byte) error {
	return writeFrame(w, payload, maxEncodeLength)
}

// writeFrame is WriteFrame with an injectable limit so the rejection path is
// testable without allocating multi-gigabyte payloads.
func writeFrame(w io.Writer, payload []byte, maxLen uint64) error {
	if uint64(len(payload)) > maxLen {
		return fmt.Errorf("%w: payload %d bytes > limit %d", ErrFrameTooLarge, len(payload), maxLen)
	}
	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, payload)
}

// writeFull writes p completely, tolerating writers that return short
// writes, and surfaces the writer's errors unchanged.
func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			// A conforming Writer never returns n < len(p) with a nil
			// error; guard against it instead of spinning forever.
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

// ReadFrame reads one frame from r: a 4-byte little-endian length prefix
// followed by that many payload bytes. The returned slice is freshly
// allocated and its size is bounded by max; allocation happens only after
// the declared length has been validated, so a hostile header cannot trigger
// an allocation bomb.
//
// Error contract:
//   - io.EOF: the stream ended cleanly on a frame boundary (zero bytes
//     read). The stream is still at EOF; a later call reports the same.
//   - io.ErrUnexpectedEOF: the stream ended mid-frame (partial header, or a
//     payload shorter than declared). The stream is dead.
//   - ErrFrameTooLarge: the declared length exceeds max. The stream is dead:
//     the oversized payload is not consumed, so the framing is misaligned.
//   - any other error: surfaced unchanged from r (deadlines, resets, ...).
//
// A max <= 0 yields ErrInvalidLimit.
func ReadFrame(r io.Reader, max int) ([]byte, error) {
	if max <= 0 {
		return nil, ErrInvalidLimit
	}
	var header [headerSize]byte
	// ReadFull distinguishes the two EOF cases itself: io.EOF when zero
	// bytes were read (clean boundary), io.ErrUnexpectedEOF when the
	// header is partial (mid-frame).
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header[:])
	if uint64(length) > uint64(max) {
		return nil, fmt.Errorf("%w: declared %d bytes > limit %d", ErrFrameTooLarge, length, max)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			// The header was consumed, so the stream cannot end
			// cleanly here: zero payload bytes before a close is a
			// mid-frame truncation, not a boundary.
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}
