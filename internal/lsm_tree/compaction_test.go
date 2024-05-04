package lsm_tree

import (
	"fmt"
	"path/filepath"
	"reflect"
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
		First: 1,
		Last:  3,
		Entries: []*SSTableEntry[int32, string]{
			{Operation: INSERT, Key: 1, Value: "value1"},
			{Operation: INSERT, Key: 2, Value: "value2"},
			{Operation: DELETE, Key: 3, Value: ""},
		},
	}
	sstable2 := &SSTable[int32, string]{
		First: 4,
		Last:  9,
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
	merged := mergeTables(level.Tables...)

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
		splits := splitTable(merged, c.SSTable_max_size, &NewTableOpts[int32, string]{
			BloomOpts: &BloomFilterOpts{
				Size: 10000,
				Path: opts.MemTableOpts.BloomPath,
			},
		})
		if len(splits) != 5 {
			t.Errorf("Number of output tables should be 5, got %v", len(splits))
		}
		for _, table := range splits {
			// fmt.Printf("%v", table.Entries)
			if table.First != table.Entries[0].Key || table.Last != table.Entries[len(table.Entries)-1].Key {
				t.Errorf("First: %v | Last: %v | First entry: %v | Last entry: %v", table.First, table.Last, table.Entries[0].Key, table.Entries[len(table.Entries)-1].Key)
			}
		}

		splits2 := splitTable(merged, 3, &NewTableOpts[int32, string]{
			BloomOpts: &BloomFilterOpts{
				Size: 10000,
				Path: opts.MemTableOpts.BloomPath,
			},
		})
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

func TestFindOverlappingTables(t *testing.T) {
	t1 := &SSTable[int32, string]{
		First: 1,
		Last:  3,
		Entries: []*SSTableEntry[int32, string]{
			{Operation: INSERT, Key: 1, Value: "value1"},
			{Operation: INSERT, Key: 2, Value: "value2"},
			{Operation: DELETE, Key: 3, Value: ""},
		},
	}
	l2 := newTestLevel()

	t.Run("Overlap one table", func(t *testing.T) {
		tbls := findOverlappingSSTables(t1, l2)

		if len(tbls) != 1 {
			for _, table := range tbls {
				t.Log(fmt.Sprintf("%v", table.Entries))
			}
			t.Errorf("Expected 1 table, found %v", len(tbls))
		}

		if !reflect.DeepEqual(t1, tbls[0]) {
			t.Error("Should be the same table")
		}
	})

	t.Run("Empty lower level", func(t *testing.T) {
		emptyLevel := &Level[int32, string]{}
		tbls := findOverlappingSSTables(t1, emptyLevel)
		if len(tbls) != 0 {
			t.Error("Should return 0 overlapping tables")
		}
	})

	t.Run("Overlap more than one table", func(t *testing.T) {
		wideTable := &SSTable[int32, string]{
			First: 1,
			Last:  5,
			Entries: []*SSTableEntry[int32, string]{
				{Operation: INSERT, Key: 1, Value: "value1"},
				{Operation: INSERT, Key: 2, Value: "value2"},
				{Operation: DELETE, Key: 3, Value: ""},
				{Operation: INSERT, Key: 4, Value: "value3"},
				{Operation: INSERT, Key: 5, Value: "value4"},
			},
		}
		tbls := findOverlappingSSTables(wideTable, newTestLevel())
		if len(tbls) != 2 {
			t.Error("Should overlap two tables")
		}
	})
}

func TestFindOldestTable(t *testing.T) {
	t.Run("4 element slice", func(t *testing.T) {
		now := time.Now()
		tables := []*SSTable[int64, string]{
			{CreatedOn: now},
			{CreatedOn: now.Add(-time.Hour * 24)},     // 1 day ago
			{CreatedOn: now.Add(-time.Hour * 24 * 2)}, // 2 days ago
			{CreatedOn: now.Add(-time.Hour * 24 * 7)}, // 7 days ago
		}

		expectedOldest := tables[3]

		oldest := findOldestTable(tables)

		if !reflect.DeepEqual(oldest, expectedOldest) {
			t.Errorf("Expected oldest table to be %v, but got %v", expectedOldest, oldest)
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Should panic on empty slice")
			}
		}()
		tables := []*SSTable[int64, string]{}
		_ = findOldestTable(tables)
	})

	t.Run("one element slice", func(t *testing.T) {
		tables := []*SSTable[int64, string]{{CreatedOn: time.Now()}}
		oldest := findOldestTable(tables)
		expectedOldest := tables[0]
		if !reflect.DeepEqual(oldest, expectedOldest) {
			t.Error("oldest element of 1-length slice should return element at index 0")
		}
	})
}
