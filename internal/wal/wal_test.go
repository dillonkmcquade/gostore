package wal

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/dillonkmcquade/gostore/internal/wal/testProtobuf"
	"google.golang.org/protobuf/proto"
)

func TestWALWrite(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := New[*testProtobuf.TestEntry](filepath.Join(tmpdir, "wal.db"), 1)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	err = wal.Write(&testProtobuf.TestEntry{
		Name: "TEST",
	})
	if err != nil {
		t.Error("error on Write:14")
	}
}

func TestWALDecode(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := New[*testProtobuf.TestEntry](filepath.Join(tmpdir, "wal.db"), 10)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	for i := 0; i < 201; i++ {
		err = wal.Write(&testProtobuf.TestEntry{Name: "TEST"})
		if err != nil {
			t.Error(err)
		}
	}

	file, err := os.Open(filepath.Join(tmpdir, "wal.db"))
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(SplitProtobuf)

	for scanner.Scan() {
		var entry testProtobuf.TestEntry
		err := proto.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			t.Error(err)
		}
	}
	if err = scanner.Err(); err != nil {
		t.Error(err)
	}
}

func TestWALDiscard(t *testing.T) {
	tmpdir := t.TempDir()
	wal, err := New[*testProtobuf.TestEntry](filepath.Join(tmpdir, "wal.dat"), 10)
	if err != nil {
		t.Error(err)
	}
	defer wal.Close()
	for i := 0; i < 25; i++ {
		err = wal.Write(&testProtobuf.TestEntry{Name: "TEST"})
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
