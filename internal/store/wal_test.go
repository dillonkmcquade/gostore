package store

import (
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWrite(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := NewWal[int, any](filepath.Join(tmpdir, "wal.db"))
	defer wal.Close()
	if err != nil {
		t.Error(err)
	}
	err = wal.Write(5, "Helloworld")
	if err != nil {
		t.Error("error on Write:14")
	}
}

func TestDecode(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := NewWal[int, any](filepath.Join(tmpdir, "wal.db"))
	defer wal.Close()
	if err != nil {
		t.Error(err)
	}
	err = wal.Write(5, "Helloworld")
	if err != nil {
		t.Error(err)
	}

	file, err := os.Open(filepath.Join(tmpdir, "wal.db"))
	defer file.Close()
	if err != nil {
		t.Error(err)
	}
	dec := gob.NewDecoder(file)

	var entry LogEntry[int, any]
	err = dec.Decode(&entry)
	if err != nil && err != io.EOF {
		t.Error(err)
	}
	if entry.Key != 5 {
		t.Error("Should be 5")
	}
}
