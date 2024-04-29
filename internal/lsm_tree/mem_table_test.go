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
		Max_size: 50,
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
		Max_size: 2000,
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

	t.Run("Delete", func(t *testing.T) {
		mem.Delete(0)
		_, found := mem.Get(0)
		if found {
			t.Error("Should have been deleted")
		}
		mem.Delete(1)
		_, found = mem.Get(1)
		if found {
			t.Error("Should have been deleted")
		}
		mem.Delete(2)
		_, found = mem.Get(2)
		if found {
			t.Error("Should have been deleted")
		}
	})
}

func TestMemTableIO(t *testing.T) {
	tmp := t.TempDir()
	wal := filepath.Join(tmp, "wal.dat")

	mem, err := NewGostoreMemTable[int64, string](&GoStoreMemTableOpts{
		walPath:  wal,
		Max_size: 2000,
	})
	defer mem.Close()

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 100; i++ {
		err = mem.Put(int64(i), "TESTVALUE")
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("Test Replay", func(t *testing.T) {
		mem2, err := NewGostoreMemTable[int64, string](&GoStoreMemTableOpts{
			walPath:  wal,
			Max_size: 2000,
		})
		if err != nil {
			t.Error(err)
		}
		defer mem2.Close()

		if mem2.Size() != 100 {
			t.Error("Should be empty memtable")
		}

		// Restore state from first memtable into second memtable

		val, found := mem2.Get(50)
		if !found || val != "TESTVALUE" {
			t.Error("Should be in memtable and value should be TESTVALUE")
		}
	})

	t.Run("Test snapshot", func(t *testing.T) {
		tbl := mem.Snapshot(tmp)
		if len(tbl.Entries) != int(mem.Size()) {
			t.Error("Should have same amount of entries")
		}

		if _, found := tbl.Search(5); !found {
			t.Error("5 Should be an entry in the SSTable")
		}
		if tbl.First != 0 || tbl.Last != 99 {
			t.Error("First or last are incorrect")
		}
	})
}
