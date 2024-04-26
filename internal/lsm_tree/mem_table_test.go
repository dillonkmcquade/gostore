package lsm_tree

import (
	"path/filepath"
	"testing"
)

func TestNewMemTable(t *testing.T) {
	tmp := t.TempDir()
	wal := filepath.Join(tmp, "wal.dat")

	mem, err := NewGostoreMemTable[int64, string](&GoStoreMemTableOpts{
		walPath:  wal,
		max_size: 50,
	})
	if err != nil {
		t.Error(err)
	}
	defer mem.Close()
}
