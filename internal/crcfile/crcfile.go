// Package crcfile writes small critical files atomically with a CRC32C
// sidecar: data is written to a temp file, fsynced, renamed, and the
// directory fsynced; the sidecar (<path>.crc) holds the hex checksum.
// Loading refuses corrupt or incomplete pairs — callers decide whether that
// is recoverable or fatal.
package crcfile

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Write stores data at path (atomic + fsync) and the checksum at path.crc.
func Write(path string, data []byte) error {
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
	crc := fmt.Sprintf("%08x\n", crc32.Checksum(data, castagnoli))
	if err := writeRaw(path+".crc", []byte(crc)); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Load reads path and verifies its sidecar CRC. A missing pair returns
// os.ErrNotExist; any mismatch is an error naming the file.
func Load(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	crcData, err := os.ReadFile(path + ".crc")
	if err != nil {
		return nil, fmt.Errorf("crcfile: %s is incomplete (no sidecar): %w", path, err)
	}
	want, err := strconv.ParseUint(strings.TrimSpace(string(crcData)), 16, 64)
	if err != nil {
		return nil, fmt.Errorf("crcfile: %s.crc is corrupt: %w", path, err)
	}
	if crc32.Checksum(data, castagnoli) != uint32(want) {
		return nil, fmt.Errorf("crcfile: %s is corrupt (CRC mismatch)", path)
	}
	return data, nil
}

func writeRaw(path string, data []byte) error {
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
