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

func TestMemTableCRUD(t *testing.T) {
	tmp := t.TempDir()
	wal := filepath.Join(tmp, "wal.dat")

	mem, err := NewGostoreMemTable[int64, string](&GoStoreMemTableOpts{
		walPath:  wal,
		max_size: 2000,
	})
	if err != nil {
		t.Error(err)
	}
	defer mem.Close()

	t.Run("Put", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			err = mem.Put(int64(i), "TESTVALUE")
			if err != nil {
				t.Error(err)
			}
		}
		if mem.Size() != 100 {
			t.Error("Size should be 100")
		}
	})
	t.Run("Get", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			val, found := mem.Get(int64(i))
			if !found {
				t.Error(err)
			}
			if val != "TESTVALUE" {
				t.Error("Val should be TESTVALUE")
			}
		}

		for i := 101; i < 200; i++ {
			_, found := mem.Get(int64(i))
			if found {
				t.Error("Should not be in memtable")
			}
		}
	})

	t.Run("Update", func(t *testing.T) {
		//
		val, found := mem.Get(0) // Verify value is in table
		if val != "TESTVALUE" || !found {
			t.Error("Should be in memtable")
		}

		err = mem.Put(0, "CHANGED") // Update value
		if err != nil {
			t.Error(err)
		}

		val, found = mem.Get(0) // Check to see that it changed
		if !found || val != "CHANGED" {
			t.Error("Error when updating value")
		}
	})
	// TODO
	t.Run("Delete", func(t *testing.T) {
		mem.Delete(0)
		_, found := mem.Get(0)
		if found {
			t.Error("Should have been deleted")
		}
	})
}

func TestMemTableReplay(t *testing.T) {
}

func TestMemTableSnapshot(t *testing.T) {
}
