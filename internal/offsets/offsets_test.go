package offsets

import (
	"bytes"
	"errors"
	"testing"
)

func TestCommitReplayAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, "app")
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 5; i++ {
		if err := m.Commit(Record{Topic: "events", Partition: i, Offset: uint64(100 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Last write per partition wins.
	if err := m.Commit(Record{Topic: "events", Partition: 2, Offset: 999}); err != nil {
		t.Fatal(err)
	}
	m.Close()

	m2, err := OpenManager(dir, "app")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer m2.Close()
	want := map[int32]uint64{0: 100, 1: 101, 2: 999, 3: 103, 4: 104}
	got := m2.All()
	if len(got) != len(want) {
		t.Fatalf("replayed %d commits, want %d", len(got), len(want))
	}
	for p, off := range want {
		if got[p] != off {
			t.Errorf("partition %d: replayed %d, want %d", p, got[p], off)
		}
		if v, ok := m2.Get(p); !ok || v != off {
			t.Errorf("Get(%d) = (%d, %v)", p, v, ok)
		}
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	recs := []Record{
		{Topic: "events", Partition: 0, Offset: 0},
		{Topic: "topics.with.dots-and_underscores", Partition: 255, Offset: 1 << 40},
		{Topic: "empty-offsets", Partition: 1<<31 - 1, Offset: 1<<64 - 1},
	}
	for _, rec := range recs {
		payload, err := encodePayload(rec)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePayload(payload)
		if err != nil {
			t.Fatal(err)
		}
		if got != rec {
			t.Errorf("round trip: got %+v, want %+v", got, rec)
		}
	}
}

func TestPayloadValidation(t *testing.T) {
	if _, err := encodePayload(Record{Topic: "t", Partition: -1}); err == nil {
		t.Error("negative partition accepted")
	}
	if _, err := decodePayload([]byte{1, 2, 3}); !errors.Is(err, ErrMalformedPayload) {
		t.Errorf("short payload: %v", err)
	}
	payload, _ := encodePayload(Record{Topic: "t", Partition: 1, Offset: 2})
	payload[0] = 9
	if _, err := decodePayload(payload); !errors.Is(err, ErrUnsupportedPayload) {
		t.Errorf("bad version: %v", err)
	}
	payload, _ = encodePayload(Record{Topic: "t", Partition: 1, Offset: 2})
	payload[13] ^= 0xFF // topic length now inconsistent
	if _, err := decodePayload(payload); !errors.Is(err, ErrMalformedPayload) {
		t.Errorf("inconsistent topic length: %v", err)
	}
}

// FuzzCommitPayload: the decoder must never panic and must round-trip
// exactly on accepted payloads.
func FuzzCommitPayload(f *testing.F) {
	f.Add([]byte{1, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 't'})
	f.Add([]byte{2, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		rec, err := decodePayload(data)
		switch {
		case err == nil:
			again, err := encodePayload(rec)
			if err != nil {
				t.Fatalf("accepted payload re-encode failed: %v", err)
			}
			if !bytes.Equal(again, data) {
				t.Fatalf("round-trip mismatch: %x vs %x", again, data)
			}
		case errors.Is(err, ErrMalformedPayload), errors.Is(err, ErrUnsupportedPayload):
			// documented taxonomy
		default:
			t.Fatalf("unexpected error %v", err)
		}
	})
}
