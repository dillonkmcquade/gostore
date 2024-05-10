package memtable

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/filter"
)

func TestNewMemTable(t *testing.T) {
	tmp := t.TempDir()
	wal := filepath.Join(tmp, "wal.dat")

	mem, err := New(&Opts{
		WalPath:          wal,
		Max_size:         50,
		Batch_write_size: 10,
		FilterOpts: &filter.Opts{
			Path: filepath.Join(tmp, "filters"),
			Size: 1000,
		},
	})
	if err != nil {
		t.Error(err)
	}
	defer mem.Close()
}

func TestMemTableCRUD(t *testing.T) {
	tmp := t.TempDir()
	wal := filepath.Join(tmp, "wal.dat")

	mem, err := New(&Opts{
		Batch_write_size: 10,
		WalPath:          wal,
		Max_size:         2000,
		LevelZero:        filepath.Join(tmp, "l0"),
		FilterOpts: &filter.Opts{
			Path: filepath.Join(tmp, "filters"),
			Size: 1000,
		},
	})
	if err != nil {
		t.Error(err)
	}
	defer mem.Close()
	for i := 0; i < 100; i++ {
		err = mem.Put([]byte(fmt.Sprintf("%v", i)), []byte("TESTVALUE"))
		if err != nil {
			t.Error(err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	if mem.Size() != 100 {
		t.Errorf("Size should be 100, found %v", mem.Size())
	}
	for i := 0; i < 100; i++ {
		val, found := mem.Get([]byte(fmt.Sprintf("%v", i)))
		if !found {
			t.Error(err)
		}
		if slices.Compare(val, []byte("TESTVALUE")) != 0 {
			t.Error("Val should be TESTVALUE")
		}
	}

	for i := 101; i < 200; i++ {
		_, found := mem.Get([]byte(fmt.Sprintf("%v", i)))
		if found {
			t.Error("Should not be in memtable")
		}
	}

	_, found := mem.Get([]byte(fmt.Sprintf("%v", 0))) // Verify value is in table
	if !found {
		t.Error("Should be in memtable")
	}

	err = mem.Put([]byte(fmt.Sprintf("%v", 0)), []byte("CHANGED")) // Update value
	if err != nil {
		t.Errorf("mem.Put: %s", err.Error())
	}

	time.Sleep(500 * time.Millisecond)
	val2, found := mem.Get([]byte(fmt.Sprintf("%v", 0))) // Check to see that it changed
	if !found {
		t.Error("Should be found")
	}
	if slices.Compare(val2, []byte("CHANGED")) != 0 {
		t.Errorf("Expected 'CHANGED', found %s", string(val2))
	}

	mem.Delete([]byte(fmt.Sprintf("%v", 0)))
	time.Sleep(500 * time.Millisecond)
	_, found = mem.Get([]byte(fmt.Sprintf("%v", 0)))
	if found {
		t.Errorf("Should have been deleted: %v", 0)
	}
	mem.Delete([]byte(fmt.Sprintf("%v", 1)))
	time.Sleep(10 * time.Millisecond)
	_, found = mem.Get([]byte(fmt.Sprintf("%v", 1)))
	if found {
		t.Errorf("Should have been deleted: %v", 1)
	}
	mem.Delete([]byte(fmt.Sprintf("%v", 2)))
	time.Sleep(10 * time.Millisecond)
	_, found = mem.Get([]byte(fmt.Sprintf("%v", 2)))
	if found {
		t.Errorf("Should have been deleted: %v", 2)
	}
}

func TestMemTableIO(t *testing.T) {
	tmp := t.TempDir()
	wal := filepath.Join(tmp, "wal.dat")

	mem, err := New(&Opts{
		Batch_write_size: 10,
		WalPath:          wal,
		Max_size:         1000,
		LevelZero:        filepath.Join(tmp, "l0"),
		FilterOpts: &filter.Opts{
			Path: filepath.Join(tmp, "filters"),
			Size: 1000,
		},
	})
	defer mem.Close()

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 200; i++ {
		err = mem.Put([]byte(fmt.Sprintf("%v", i)), []byte("TESTVALUE"))
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("Test Replay", func(t *testing.T) {
		mem2, err := New(&Opts{
			WalPath:          wal,
			Max_size:         1000,
			Batch_write_size: 10,
			LevelZero:        filepath.Join(tmp, "l0"),
			FilterOpts: &filter.Opts{
				Path: filepath.Join(tmp, "filters"),
				Size: 1000,
			},
		})
		if err != nil {
			t.Error(err)
		}
		defer mem2.Close()

		if mem2.Size() != 200 {
			t.Error("Should be empty memtable")
		}

		// Restore state from first memtable into second memtable

		_, found := mem2.Get([]byte(fmt.Sprintf("%v", 50)))
		if !found {
			t.Error("Should be in memtable and value should be TESTVALUE")
		}
	})

	// t.Run("Test snapshot", func(t *testing.T) {
	// 	tbl := mem.Snapshot()
	// 	if len(tbl.Entries) != int(mem.Size()) {
	// 		t.Error("Should have same amount of entries")
	// 	}
	//
	// 	if _, found := tbl.Search(5); !found {
	// 		t.Error("5 Should be an entry in the SSTable")
	// 	}
	// 	if tbl.First != 0 || tbl.Last != 199 {
	// 		t.Error("First or last are incorrect")
	// 	}
	// })
}
