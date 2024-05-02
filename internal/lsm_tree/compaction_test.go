package lsm_tree

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestLevel0Compaction(t *testing.T) {
	tmp := t.TempDir()

	opts := NewTestLSMOpts(tmp)
	tree := New[int64, string](opts)

	defer tree.Close()

	for i := 0; i < 10000; i++ {
		err := tree.Write(int64(i), "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE")
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(1999)
		if err != nil {
			t.Errorf("Reading %v: %v", 1999, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(0)
		if err != nil {
			t.Errorf("Reading %v: %v", 0, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(2000)
		if err != nil {
			t.Errorf("Reading %v: %v", 2000, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(3000)
		if err != nil {
			t.Errorf("Reading %v: %v", 3000, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(8000)
		if err != nil {
			t.Errorf("Reading %v: %v", 8000, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(1111)
		if err != nil {
			t.Errorf("Reading %v: %v", 1111, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree", func(t *testing.T) {
		val, err := tree.Read(8888)
		if err != nil {
			t.Errorf("Reading %v: %v", 8888, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
}

func newTestLevel() *Level[int32, string] {
	sstable1 := &SSTable[int32, string]{
		Entries: []*SSTableEntry[int32, string]{
			{Operation: INSERT, Key: 1, Value: "value1"},
			{Operation: INSERT, Key: 2, Value: "value2"},
			{Operation: DELETE, Key: 3, Value: ""},
		},
	}
	sstable2 := &SSTable[int32, string]{
		Entries: []*SSTableEntry[int32, string]{
			{Operation: INSERT, Key: 4, Value: "value3"},
			{Operation: INSERT, Key: 5, Value: "value4"},
			{Operation: INSERT, Key: 6, Value: "value4"},
			{Operation: INSERT, Key: 7, Value: "value4"},
			{Operation: INSERT, Key: 8, Value: "value4"},
			{Operation: INSERT, Key: 9, Value: "value4"},
		},
	}
	return &Level[int32, string]{
		Number:  0,
		Tables:  []*SSTable[int32, string]{sstable1, sstable2},
		MaxSize: 3,
	}
}

func TestCompactionMerge(t *testing.T) {
	tmp := t.TempDir()
	opts := NewTestLSMOpts(tmp)
	// Create some sample SSTables
	level := newTestLevel()

	c := &CompactionImpl[int32, string]{
		LevelPaths:       opts.LevelPaths,
		SSTable_max_size: 2,
		BloomPath:        tmp,
	}

	timestamp := time.Now()
	merged := c.merge(level.Tables...)

	t.Run("Test merge", func(t *testing.T) {
		if len(merged.Entries) != (len(level.Tables[0].Entries) + len(level.Tables[1].Entries)) {
			t.Error("Merged size should equal size of both tables summed")
		}

		merged.Name = filepath.Join(tmp, fmt.Sprintf("%v.segment", timestamp.Unix()))
		size, err := merged.Sync()

		if err != nil || size == 0 {
			t.Error(err)
		}
		if merged.First != 1 || merged.Last != 9 {
			t.Error("First/Last should be 1 & 9")
		}
	})

	t.Run("Test split", func(t *testing.T) {
		merged.Open()
		defer merged.Close()
		splits := c.split(merged)
		if len(splits) != 5 {
			t.Errorf("Number of output tables should be 5, got %v", len(splits))
		}
		for _, table := range splits {
			// fmt.Printf("%v", table.Entries)
			if table.First != table.Entries[0].Key || table.Last != table.Entries[len(table.Entries)-1].Key {
				t.Errorf("First: %v | Last: %v | First entry: %v | Last entry: %v", table.First, table.Last, table.Entries[0].Key, table.Entries[len(table.Entries)-1].Key)
			}
		}

		splits2 := c.split(level.Tables[1])
		if len(splits2) != 3 {
			t.Errorf("Number of output tables should be 3, got %v", len(splits))
		}
		for _, table := range splits2 {
			// fmt.Printf("%v", table.Entries)
			if table.First != table.Entries[0].Key || table.Last != table.Entries[len(table.Entries)-1].Key {
				t.Errorf("First: %v | Last: %v | First entry: %v | Last entry: %v", table.First, table.Last, table.Entries[0].Key, table.Entries[len(table.Entries)-1].Key)
			}
		}
	})
}
