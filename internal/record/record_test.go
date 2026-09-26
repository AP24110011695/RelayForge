package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"testing"
)

// goldenRecord is the fixed record the golden test pins.
func goldenRecord() Record {
	return Record{Key: []byte("key"), Value: []byte("value"), Timestamp: 1234567890123}
}

// TestGoldenEncoding pins the byte layout with an independently constructed
// byte image: every field position and value is built here from the format
// spec (docs/RECORD_FORMAT.md), not from the implementation under test.
func TestGoldenEncoding(t *testing.T) {
	var buf bytes.Buffer
	if err := goldenRecord().EncodeTo(&buf, 1024); err != nil {
		t.Fatalf("EncodeTo: %v", err)
	}
	got := buf.Bytes()

	const keyLen, valLen = 3, 5
	const total = 26 + keyLen + valLen // 34
	want := make([]byte, total)
	binary.LittleEndian.PutUint32(want[0:4], total-4) // frame length: 30
	want[8] = 1                                       // version
	want[9] = 0                                       // attributes (reserved)
	binary.LittleEndian.PutUint64(want[10:18], 1234567890123)
	binary.LittleEndian.PutUint32(want[18:22], keyLen)
	copy(want[22:22+keyLen], "key")
	binary.LittleEndian.PutUint32(want[22+keyLen:22+keyLen+4], valLen)
	copy(want[22+keyLen+4:], "value")
	binary.LittleEndian.PutUint32(want[4:8], crc32.Checksum(want[8:], castagnoli))

	if !bytes.Equal(got, want) {
		t.Fatalf("encoding mismatch:\n got %x\nwant %x", got, want)
	}
	if len(got) != total {
		t.Errorf("encoded size = %d, want %d", len(got), total)
	}
	if frameLen := binary.LittleEndian.Uint32(got[0:4]); frameLen != 30 {
		t.Errorf("frame length = %d, want 30", frameLen)
	}
}

func TestRoundTrip(t *testing.T) {
	rnd := rand.New(rand.NewSource(3))
	big := make([]byte, 1024*1024)
	rnd.Read(big)
	cases := []Record{
		{},
		{Timestamp: 1},
		{Key: []byte("k")},
		{Value: []byte("v")},
		{Key: []byte("key"), Value: []byte("value"), Timestamp: 1234567890123},
		{Key: bytes.Repeat([]byte{0xAA}, 1000), Value: []byte("negative timestamp"), Timestamp: -5},
		{Value: big},
		{Key: bytes.Repeat([]byte{0xBB}, 1024*1024), Value: []byte("1 MiB key")},
	}
	for i, rec := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var buf bytes.Buffer
			if err := rec.EncodeTo(&buf, 4<<20); err != nil {
				t.Fatalf("EncodeTo: %v", err)
			}
			decoded, n, err := Decode(buf.Bytes(), 4<<20)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if n != buf.Len() {
				t.Errorf("consumed %d bytes, encoded %d", n, buf.Len())
			}
			if !bytes.Equal(decoded.Key, rec.Key) || !bytes.Equal(decoded.Value, rec.Value) ||
				decoded.Timestamp != rec.Timestamp || decoded.Attributes != rec.Attributes {
				t.Error("slice round-trip mismatch")
			}
			streamed, n2, err := DecodeFrom(bytes.NewReader(buf.Bytes()), 4<<20)
			if err != nil {
				t.Fatalf("DecodeFrom: %v", err)
			}
			if n2 != buf.Len() {
				t.Errorf("stream consumed %d bytes, encoded %d", n2, buf.Len())
			}
			if !bytes.Equal(streamed.Key, rec.Key) || !bytes.Equal(streamed.Value, rec.Value) ||
				streamed.Timestamp != rec.Timestamp {
				t.Error("stream round-trip mismatch")
			}
			if _, _, err := DecodeFrom(bytes.NewReader(buf.Bytes()[n2:]), 4<<20); !errors.Is(err, io.EOF) {
				t.Errorf("decode past the record: error = %v, want io.EOF", err)
			}
		})
	}
}

// TestDecodeTruncationMatrix truncates a valid record at every byte offset:
// io.EOF only at cut 0 (clean boundary), io.ErrUnexpectedEOF everywhere else,
// never a decoded record.
func TestDecodeTruncationMatrix(t *testing.T) {
	frame := mustEncode(t, goldenRecord())
	for cut := 0; cut < len(frame); cut++ {
		_, n, err := Decode(frame[:cut], 4096)
		if err == nil {
			t.Fatalf("cut %d: expected error, got record consuming %d bytes", cut, n)
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

// TestCRCMatrix flips every byte from the CRC field onward: any flip must
// break the checksum check.
func TestCRCMatrix(t *testing.T) {
	frame := mustEncode(t, goldenRecord())
	for pos := 4; pos < len(frame); pos++ {
		corrupt := append([]byte(nil), frame...)
		corrupt[pos] ^= 0xFF
		_, _, err := Decode(corrupt, 4096)
		if !errors.Is(err, ErrChecksumMismatch) {
			t.Errorf("flip at %d: error = %v, want ErrChecksumMismatch", pos, err)
		}
	}
}

// fixCRC recomputes the CRC field after a targeted field mutation, so the
// corruption check passes and the mutation reaches the field's own check.
func fixCRC(frame []byte) {
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Checksum(frame[8:], castagnoli))
}

func TestVersionRejection(t *testing.T) {
	for _, v := range []byte{0, 2, 255} {
		frame := mustEncode(t, goldenRecord())
		frame[8] = v
		fixCRC(frame)
		if _, _, err := Decode(frame, 4096); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("version %d: error = %v, want ErrUnsupportedVersion", v, err)
		}
	}
}

func TestReservedAttributes(t *testing.T) {
	frame := mustEncode(t, goldenRecord())
	frame[9] = 1
	fixCRC(frame)
	if _, _, err := Decode(frame, 4096); !errors.Is(err, ErrReservedAttributes) {
		t.Errorf("decode: error = %v, want ErrReservedAttributes", err)
	}
	if _, err := (Record{Value: []byte("x"), Attributes: 1}).Encode(4096); !errors.Is(err, ErrReservedAttributes) {
		t.Errorf("encode: error = %v, want ErrReservedAttributes", err)
	}
}

func TestMalformedLengths(t *testing.T) {
	// Frame length below the fixed minimum (not CRC-protected).
	frame := mustEncode(t, goldenRecord())
	binary.LittleEndian.PutUint32(frame[0:4], 10)
	if _, _, err := Decode(frame, 4096); !errors.Is(err, ErrMalformed) {
		t.Errorf("short frame length: error = %v, want ErrMalformed", err)
	}

	// Key length running past the frame.
	frame = mustEncode(t, goldenRecord())
	binary.LittleEndian.PutUint32(frame[18:22], 1000)
	fixCRC(frame)
	if _, _, err := Decode(frame, 4096); !errors.Is(err, ErrMalformed) {
		t.Errorf("oversized key length: error = %v, want ErrMalformed", err)
	}

	// Value length inconsistent with the frame.
	frame = mustEncode(t, goldenRecord())
	binary.LittleEndian.PutUint32(frame[25:29], 6)
	fixCRC(frame)
	if _, _, err := Decode(frame, 4096); !errors.Is(err, ErrMalformed) {
		t.Errorf("inconsistent value length: error = %v, want ErrMalformed", err)
	}
}

func TestSizeLimit(t *testing.T) {
	frame := mustEncode(t, goldenRecord()) // 34 bytes

	if _, _, err := Decode(frame, 20); !errors.Is(err, ErrRecordTooLarge) {
		t.Errorf("decode with small limit: error = %v, want ErrRecordTooLarge", err)
	}
	if _, err := goldenRecord().Encode(20); !errors.Is(err, ErrRecordTooLarge) {
		t.Errorf("encode over limit: error = %v, want ErrRecordTooLarge", err)
	}
	for _, limit := range []int{0, -1} {
		if _, err := goldenRecord().Encode(limit); !errors.Is(err, ErrInvalidLimit) {
			t.Errorf("encode limit %d: error = %v, want ErrInvalidLimit", limit, err)
		}
		if _, _, err := Decode(frame, limit); !errors.Is(err, ErrInvalidLimit) {
			t.Errorf("decode limit %d: error = %v, want ErrInvalidLimit", limit, err)
		}
	}
	// Exactly at the limit must succeed.
	if _, err := goldenRecord().Encode(34); err != nil {
		t.Errorf("encode at exact limit: %v", err)
	}
}

// TestDecodeAliasesInput documents the zero-copy contract: decoded Key and
// Value alias the input buffer.
func TestDecodeAliasesInput(t *testing.T) {
	data := mustEncode(t, goldenRecord())
	rec, _, err := Decode(data, 4096)
	if err != nil {
		t.Fatal(err)
	}
	rec.Key[0] = 'X'
	if data[22] != 'X' {
		t.Fatal("decoded Key does not alias the input buffer")
	}
}

func mustEncode(t *testing.T, rec Record) []byte {
	t.Helper()
	buf, err := rec.Encode(1 << 20)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return buf
}

// FuzzDecode: the decoder must never panic and must always report the
// documented taxonomy; any successful decode must re-encode to the exact
// consumed bytes.
func FuzzDecode(f *testing.F) {
	golden := mustEncodeB(goldenRecord())
	f.Add(golden, uint16(4096))
	f.Add(golden[:len(golden)-1], uint16(4096))
	f.Add(golden[:7], uint16(4096))
	f.Add([]byte{}, uint16(4096))
	f.Add([]byte{0x01, 0x02}, uint16(4096))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}, uint16(4096))
	corrupt := append([]byte(nil), golden...)
	corrupt[12] ^= 0x01
	f.Add(corrupt, uint16(4096))
	f.Add(append(append([]byte(nil), golden...), 1, 2, 3), uint16(4096))
	f.Fuzz(func(t *testing.T, data []byte, limitRaw uint16) {
		limit := int(limitRaw)%(1<<16) + 64
		rec, n, err := Decode(data, limit)
		switch {
		case err == nil:
			if n <= 0 || n > len(data) {
				t.Fatalf("consumed %d of %d bytes", n, len(data))
			}
			var buf bytes.Buffer
			if err := rec.EncodeTo(&buf, limit); err != nil {
				t.Fatalf("re-encode of accepted record failed: %v", err)
			}
			if !bytes.Equal(buf.Bytes(), data[:n]) {
				t.Fatalf("re-encode mismatch:\n got %x\nwant %x", buf.Bytes(), data[:n])
			}
		case errors.Is(err, io.EOF),
			errors.Is(err, io.ErrUnexpectedEOF),
			errors.Is(err, ErrChecksumMismatch),
			errors.Is(err, ErrUnsupportedVersion),
			errors.Is(err, ErrReservedAttributes),
			errors.Is(err, ErrMalformed),
			errors.Is(err, ErrRecordTooLarge),
			errors.Is(err, ErrInvalidLimit):
			// documented taxonomy
		default:
			t.Fatalf("unexpected error %v", err)
		}
		// The stream variant must agree with the slice variant on the
		// first record of the same input.
		_, _, streamErr := DecodeFrom(bytes.NewReader(data), limit)
		if (err == nil) != (streamErr == nil) {
			t.Fatalf("Decode and DecodeFrom disagree: %v vs %v", err, streamErr)
		}
	})
}

func mustEncodeB(rec Record) []byte {
	buf, err := rec.Encode(1 << 20)
	if err != nil {
		panic(err)
	}
	return buf
}

func BenchmarkRecordEncode(b *testing.B) {
	rec := Record{Key: []byte("bench-key"), Value: make([]byte, 1024)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := rec.Encode(4 << 20); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecordDecode(b *testing.B) {
	data := mustEncodeB(Record{Key: []byte("bench-key"), Value: make([]byte, 1024)})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := Decode(data, 4<<20); err != nil {
			b.Fatal(err)
		}
	}
}
