// Package metadata persists the broker's topic table: a CRC-protected,
// versioned JSON snapshot written atomically (temp file + fsync + rename +
// directory fsync). Metadata is critical state: a corrupt snapshot makes
// the broker refuse to start rather than guess.
package metadata

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	// MaxTopicName bounds topic names (they become directory names).
	MaxTopicName = 128
	// MaxPartitions bounds the partition count fixed at creation.
	MaxPartitions = 256
	// DefaultTopic is created automatically with one partition.
	DefaultTopic = "default"
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by the store.
var (
	// ErrTopicExists means the topic name is already taken.
	ErrTopicExists = errors.New("metadata: topic already exists")
	// ErrTopicNotFound means the topic name is unknown.
	ErrTopicNotFound = errors.New("metadata: unknown topic")
	// ErrInvalidTopic is returned for names or partition counts that
	// violate the validation rules.
	ErrInvalidTopic = errors.New("metadata: invalid topic name or partition count")
)

// Topic is one topic's fixed configuration.
type Topic struct {
	Name           string `json:"name"`
	PartitionCount int32  `json:"partition_count"`
}

type table struct {
	Topics map[string]Topic `json:"topics"`
}

// Store is the persisted topic table. Safe for concurrent use.
type Store struct {
	mu  sync.Mutex
	dir string
	tab table
}

// Open loads (or initializes) the metadata snapshot under dataDir.
func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dataDir, tab: table{Topics: map[string]Topic{}}}
	data, jerr := os.ReadFile(s.jsonPath())
	crcData, cerr := os.ReadFile(s.crcPath())
	switch {
	case os.IsNotExist(jerr) && os.IsNotExist(cerr):
		if err := s.persist(); err != nil {
			return nil, err
		}
		return s, nil
	case jerr != nil || cerr != nil:
		return nil, fmt.Errorf("metadata: incomplete metadata in %s (json err %v, crc err %v): manual inspection required",
			dataDir, jerr, cerr)
	}
	want, err := strconv.ParseUint(strings.TrimSpace(string(crcData)), 16, 64)
	if err != nil {
		return nil, fmt.Errorf("metadata: %s is corrupt: %w", s.crcPath(), err)
	}
	if crc32.Checksum(data, castagnoli) != uint32(want) {
		return nil, fmt.Errorf("metadata: %s is corrupt (CRC mismatch): manual inspection required", s.jsonPath())
	}
	if err := json.Unmarshal(data, &s.tab); err != nil {
		return nil, fmt.Errorf("metadata: %s is corrupt: %w", s.jsonPath(), err)
	}
	if s.tab.Topics == nil {
		s.tab.Topics = map[string]Topic{}
	}
	return s, nil
}

func (s *Store) jsonPath() string { return filepath.Join(s.dir, "metadata.json") }
func (s *Store) crcPath() string  { return filepath.Join(s.dir, "metadata.crc") }

// Create adds a topic with a fixed partition count and persists it.
func (s *Store) Create(name string, partitionCount int32) error {
	if err := validate(name, partitionCount); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tab.Topics[name]; ok {
		return fmt.Errorf("%w: %q", ErrTopicExists, name)
	}
	backup := s.tab
	s.tab.Topics[name] = Topic{Name: name, PartitionCount: partitionCount}
	if err := s.persist(); err != nil {
		s.tab = backup
		return err
	}
	return nil
}

// Get returns a topic by name.
func (s *Store) Get(name string) (Topic, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tab.Topics[name]
	return t, ok
}

// Dir returns the directory the snapshot lives in.
func (s *Store) Dir() string { return s.dir }

// Delete removes a topic and persists. Used only for CreateTopic rollback.
func (s *Store) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tab.Topics[name]; !ok {
		return fmt.Errorf("%w: %q", ErrTopicNotFound, name)
	}
	backup := s.tab
	delete(s.tab.Topics, name)
	if err := s.persist(); err != nil {
		s.tab = backup
		return err
	}
	return nil
}

// List returns all topics sorted by name.
func (s *Store) List() []Topic {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Topic, 0, len(s.tab.Topics))
	for _, t := range s.tab.Topics {
		out = append(out, t)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Name < out[j-1].Name; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func validate(name string, partitionCount int32) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("%w: name %q must match [A-Za-z0-9._-]{1,128}", ErrInvalidTopic, name)
	}
	if partitionCount < 1 || partitionCount > MaxPartitions {
		return fmt.Errorf("%w: partition count %d must be in [1, %d]", ErrInvalidTopic, partitionCount, MaxPartitions)
	}
	return nil
}

// persist writes the snapshot atomically: temp file + fsync + rename +
// directory fsync, for both the JSON and its CRC sidecar.
func (s *Store) persist() error {
	data, err := json.MarshalIndent(s.tab, "", "  ")
	if err != nil {
		return err
	}
	crc := fmt.Sprintf("%08x\n", crc32.Checksum(data, castagnoli))
	if err := atomicWrite(s.jsonPath(), data); err != nil {
		return err
	}
	if err := atomicWrite(s.crcPath(), []byte(crc)); err != nil {
		return err
	}
	return nil
}

func atomicWrite(path string, data []byte) error {
	suffix, err := randomSuffix()
	if err != nil {
		return err
	}
	tmp := path + ".tmp" + suffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func randomSuffix() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
