package metadata

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateListGet(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create("events", 4); err != nil {
		t.Fatal(err)
	}
	if err := s.Create("events", 2); !errors.Is(err, ErrTopicExists) {
		t.Errorf("duplicate create: %v", err)
	}
	if err := s.Create("bad name!", 1); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("invalid name: %v", err)
	}
	if err := s.Create("zero", 0); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("zero partitions: %v", err)
	}
	if err := s.Create("too-many", MaxPartitions+1); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("too many partitions: %v", err)
	}
	topics := s.List()
	if len(topics) != 1 || topics[0].Name != "events" || topics[0].PartitionCount != 4 {
		t.Fatalf("list: %+v", topics)
	}
	if got, ok := s.Get("events"); !ok || got.PartitionCount != 4 {
		t.Errorf("get: %+v ok=%v", got, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Error("get on missing topic returned true")
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create("alpha", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Create("beta", 7); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatalf("reopen: %v", err)
	} else {
		s2, _ := Open(dir)
		topics := s2.List()
		if len(topics) != 2 || topics[0].Name != "alpha" || topics[1].PartitionCount != 7 {
			t.Fatalf("reopened table: %+v", topics)
		}
	}
}

func TestCorruptSnapshotRefusesOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create("keep", 1); err != nil {
		t.Fatal(err)
	}
	// Corrupt the JSON but keep the (now stale) CRC.
	jsonPath := filepath.Join(dir, "metadata.json")
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Open(dir)
	if err == nil {
		t.Fatal("Open accepted a corrupt snapshot")
	}
	if !strings.Contains(err.Error(), "CRC mismatch") || !strings.Contains(err.Error(), "manual inspection") {
		t.Errorf("error = %v, want a CRC-mismatch refusal", err)
	}
	// Missing CRC with JSON present is also a refusal.
	os.Remove(filepath.Join(dir, "metadata.crc"))
	_, err = Open(dir)
	if err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Errorf("missing CRC: error = %v, want incomplete-metadata refusal", err)
	}
}

func TestDeleteRollback(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create("temp", 3); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("temp"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("temp"); ok {
		t.Error("deleted topic still present")
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Get("temp"); ok {
		t.Error("deleted topic persisted")
	}
}
