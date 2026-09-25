package framing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"testing/iotest"
)

var errBoom = errors.New("boom: injected transport error")

// frameBytes encodes payload and panics on error; for tests where the
// payload is known to be encodable.
func frameBytes(payload []byte) []byte {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func TestWriteFrameHeaderLayout(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte("hi")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	want := []byte{0x02, 0x00, 0x00, 0x00, 'h', 'i'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("frame = %x, want %x", buf.Bytes(), want)
	}
}

func TestRoundTrip(t *testing.T) {
	payloads := [][]byte{
		{},
		[]byte("a"),
		[]byte("hello relayforge"),
		make([]byte, 64*1024),
	}
	if _, err := rand.New(rand.NewSource(1)).Read(payloads[3]); err != nil {
		t.Fatalf("rand read: %v", err)
	}
	for _, payload := range payloads {
		t.Run(fmt.Sprintf("len-%d", len(payload)), func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			got, err := ReadFrame(&buf, 1<<20)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
			}
			if _, err := ReadFrame(&buf, 1<<20); !errors.Is(err, io.EOF) {
				t.Errorf("second ReadFrame error = %v, want io.EOF", err)
			}
		})
	}
}

func TestRoundTripPipe(t *testing.T) {
	payload := []byte("over a real pipe with concurrent writer and reader")
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(WriteFrame(pw, payload))
	}()
	got, err := ReadFrame(pr, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("payload mismatch")
	}
	if _, err := ReadFrame(pr, 1<<20); !errors.Is(err, io.EOF) {
		t.Errorf("second ReadFrame error = %v, want io.EOF", err)
	}
}

// TestReadFrameChunkedReads forces single-byte reads over a stream holding
// several frames: framing must reassemble across arbitrary read boundaries.
func TestReadFrameChunkedReads(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		if err := WriteFrame(&buf, []byte(fmt.Sprintf("frame-%d", i))); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	r := iotest.OneByteReader(&buf)
	for i := 0; i < 3; i++ {
		got, err := ReadFrame(r, 1<<20)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if want := []byte(fmt.Sprintf("frame-%d", i)); !bytes.Equal(got, want) {
			t.Errorf("frame %d = %q, want %q", i, got, want)
		}
	}
	if _, err := ReadFrame(r, 1<<20); !errors.Is(err, io.EOF) {
		t.Errorf("final error = %v, want io.EOF", err)
	}
}

// TestChunkedWritesPipe delivers one frame through a pipe in random-size
// chunks, exercising the reader against arbitrary segment boundaries.
func TestChunkedWritesPipe(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5A}, 1000)
	pr, pw := io.Pipe()
	go func() {
		frame := frameBytes(payload)
		rnd := rand.New(rand.NewSource(42))
		var err error
		for len(frame) > 0 {
			chunk := rnd.Intn(7) + 1
			if chunk > len(frame) {
				chunk = len(frame)
			}
			if _, err = pw.Write(frame[:chunk]); err != nil {
				break
			}
			frame = frame[chunk:]
		}
		pw.CloseWithError(err)
	}()
	got, err := ReadFrame(pr, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("payload mismatch after chunked writes")
	}
}

// TestReadFrameTruncationMatrix truncates a valid frame at every byte offset
// and requires the error taxonomy: io.EOF only at cut 0 (clean boundary),
// io.ErrUnexpectedEOF everywhere else, never a payload.
func TestReadFrameTruncationMatrix(t *testing.T) {
	frame := frameBytes([]byte("relayforge truncation matrix"))
	for cut := 0; cut < len(frame); cut++ {
		got, err := ReadFrame(bytes.NewReader(frame[:cut]), 1024)
		if err == nil {
			t.Fatalf("cut %d: expected error, got payload of %d bytes", cut, len(got))
		}
		if cut == 0 {
			if !errors.Is(err, io.EOF) {
				t.Errorf("cut 0: error = %v, want io.EOF", err)
			}
			continue
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("cut %d: error = %v, want io.ErrUnexpectedEOF", cut, err)
		}
	}
}

func TestReadFrameLimitBoundary(t *testing.T) {
	atLimit := frameBytes(bytes.Repeat([]byte{1}, 1024))
	got, err := ReadFrame(bytes.NewReader(atLimit), 1024)
	if err != nil {
		t.Fatalf("declared == max: %v", err)
	}
	if len(got) != 1024 {
		t.Errorf("len = %d, want 1024", len(got))
	}

	overLimit := frameBytes(bytes.Repeat([]byte{1}, 1025))
	if _, err = ReadFrame(bytes.NewReader(overLimit), 1024); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("declared max+1: error = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameAbsurdLength(t *testing.T) {
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], 0xFFFFFFFF)
	if _, err := ReadFrame(bytes.NewReader(header[:]), 64); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("error = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameInvalidLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		if _, err := ReadFrame(bytes.NewReader(nil), limit); !errors.Is(err, ErrInvalidLimit) {
			t.Errorf("limit %d: error = %v, want ErrInvalidLimit", limit, err)
		}
	}
}

func TestReadFrameTransportError(t *testing.T) {
	if _, err := ReadFrame(iotest.ErrReader(errBoom), 64); !errors.Is(err, errBoom) {
		t.Errorf("header-stage error = %v, want errBoom", err)
	}
	// Serve exactly the 4-byte header, then fail: the error must surface
	// from the payload-stage read.
	r := &errAfterHeader{buf: frameBytes([]byte("payload"))[:4]}
	if _, err := ReadFrame(r, 64); !errors.Is(err, errBoom) {
		t.Errorf("payload-stage error = %v, want errBoom", err)
	}
}

// errAfterHeader serves the buffered bytes, then fails forever.
type errAfterHeader struct{ buf []byte }

func (r *errAfterHeader) Read(p []byte) (int, error) {
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}
	return 0, errBoom
}

// shortWriter violates the io.Writer contract deliberately (short write with
// nil error); writeFull must tolerate it.
type shortWriter struct {
	w    io.Writer
	step int
}

func (s *shortWriter) Write(p []byte) (int, error) {
	n := s.step
	if n > len(p) {
		n = len(p)
	}
	return s.w.Write(p[:n])
}

func TestWriteFrameShortWriters(t *testing.T) {
	payload := []byte("survives short writes")
	for _, step := range []int{1, 3, 7} {
		var sink bytes.Buffer
		if err := WriteFrame(&shortWriter{w: &sink, step: step}, payload); err != nil {
			t.Fatalf("step %d: WriteFrame: %v", step, err)
		}
		got, err := ReadFrame(&sink, 1024)
		if err != nil || !bytes.Equal(got, payload) {
			t.Errorf("step %d: round-trip failed: %v", step, err)
		}
	}
}

type zeroWriter struct{}

func (zeroWriter) Write(p []byte) (int, error) { return 0, nil }

type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

func TestWriteFrameWriterErrors(t *testing.T) {
	if err := WriteFrame(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("zero-progress writer: error = %v, want io.ErrShortWrite", err)
	}
	if err := WriteFrame(errWriter{err: errBoom}, []byte("x")); !errors.Is(err, errBoom) {
		t.Errorf("failing writer: error = %v, want errBoom", err)
	}
}

func TestWriteFramePayloadLimit(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, make([]byte, 10), 5); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("payload > limit: error = %v, want ErrFrameTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Errorf("rejected payload must not be written, buf holds %d bytes", buf.Len())
	}
	if err := writeFrame(&buf, make([]byte, 5), 5); err != nil {
		t.Errorf("payload == limit: %v", err)
	}
}

// FuzzReadFrame: the decoder must never panic, never allocate beyond the
// limit, and always report the documented taxonomy for arbitrary input.
func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{}, uint16(64))                          // clean boundary EOF
	f.Add([]byte{0, 0, 0, 0}, uint16(64))                // empty frame
	f.Add([]byte{3, 0, 0, 0, 'a', 'b', 'c'}, uint16(64)) // valid frame
	f.Add([]byte{3, 0, 0}, uint16(64))                   // truncated header
	f.Add([]byte{3, 0, 0, 0, 'a'}, uint16(64))           // truncated payload
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, uint16(64))    // absurd length
	f.Fuzz(func(t *testing.T, data []byte, limit uint16) {
		maxSize := int(limit)%8192 + 1
		got, err := ReadFrame(bytes.NewReader(data), maxSize)
		switch {
		case err == nil:
			if len(got) > maxSize {
				t.Fatalf("returned %d bytes, limit %d", len(got), maxSize)
			}
			if 4+len(got) > len(data) {
				t.Fatalf("frame needs %d bytes, input has %d", 4+len(got), len(data))
			}
			if declared := binary.LittleEndian.Uint32(data); int(declared) != len(got) {
				t.Fatalf("declared %d, returned %d", declared, len(got))
			}
		case errors.Is(err, io.EOF):
			if len(data) != 0 {
				t.Fatalf("clean EOF on %d bytes of input", len(data))
			}
		case errors.Is(err, io.ErrUnexpectedEOF):
			// truncated mid-frame: acceptable
		case errors.Is(err, ErrFrameTooLarge):
			if len(data) < 4 {
				t.Fatal("too-large without a full header")
			}
			if declared := uint64(binary.LittleEndian.Uint32(data)); declared <= uint64(maxSize) {
				t.Fatalf("too-large for declared %d, limit %d", declared, maxSize)
			}
		default:
			t.Fatalf("unexpected error %v", err)
		}
	})
}
