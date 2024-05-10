package wal

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type TestEntry struct{}

func (t *TestEntry) Apply(c interface{}) {
}

func TestWALWrite(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := New[*TestEntry](filepath.Join(tmpdir, "wal.db"), 10)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	err = wal.Write(&TestEntry{})
	if err != nil {
		t.Error("error on Write:14")
	}
}

func TestWALDecode(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := New[*TestEntry](filepath.Join(tmpdir, "wal.db"), 10)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	for i := 0; i < 201; i++ {
		err = wal.Write(&TestEntry{})
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

	var entries []*TestEntry
	for {
		var entry []*TestEntry
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
	wal, err := New[*TestEntry](filepath.Join(tmpdir, "wal.dat"), 10)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	for i := 0; i < 25; i++ {
		err = wal.Write(&TestEntry{})
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

func TestGenerateUniqueWALName(t *testing.T) {
	n1 := generateUniqueWALName()
	n2 := generateUniqueWALName()

	if n1 == n2 {
		t.Error("Should be different")
	}
}