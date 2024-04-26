package lsm_tree

import (
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWALWrite(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := newWal[int, any](filepath.Join(tmpdir, "wal.db"))
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	err = wal.Write(5, "Helloworld")
	if err != nil {
		t.Error("error on Write:14")
	}
}

func TestWALDecode(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := newWal[int, any](filepath.Join(tmpdir, "wal.db"))
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	err = wal.Write(5, "Helloworld")
	if err != nil {
		t.Error(err)
	}

	file, err := os.Open(filepath.Join(tmpdir, "wal.db"))
	if err != nil {
		t.Error(err)
	}
	defer file.Close()
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

func TestWALDiscard(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := newWal[int, any](filepath.Join(tmpdir, "wal.dat"))
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	for i := 0; i < 25; i++ {
		err = wal.Write(i, "Helloworld")
		if err != nil {
			t.Error("error on Write:14")
		}
	}
	wal.Discard()
	size, err := wal.Size()
	if err != nil {
		t.Error(err)
	}
	if size != 0 {
		t.Errorf("File size should be 0, received %v", size)
	}
}
