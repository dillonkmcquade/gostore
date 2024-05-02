package lsm_tree

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWALWrite(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := newWal[int, any](filepath.Join(tmpdir, "wal.db"), 10)
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
	wal, err := newWal[int, any](filepath.Join(tmpdir, "wal.db"), 10)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	for i := 0; i < 201; i++ {
		err = wal.Write(i, "Helloworld")
		if err != nil {
			t.Error(err)
		}
	}

	file, err := os.Open(filepath.Join(tmpdir, "wal.db"))
	if err != nil {
		t.Error(err)
	}
	defer file.Close()
	dec := json.NewDecoder(file)

	var entries []*LogEntry[int, any]
	for {
		var entry []*LogEntry[int, any]
		if err = dec.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			} else {
				t.Error(err)
			}
		}
		entries = append(entries, entry...)
	}
	if len(entries) != 200 {
		t.Errorf("Should have decoded 200 entries, received %v", len(entries))
	}
}

func TestWALDiscard(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := newWal[int, any](filepath.Join(tmpdir, "wal.dat"), 10)
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
