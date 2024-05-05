package sstable

import (
	"reflect"
	"testing"
	"time"
)

var t1 = &SSTable[int32, string]{
	First: 1,
	Last:  3,
	Entries: []*Entry[int32, string]{
		{Operation: INSERT, Key: 1, Value: "value1"},
		{Operation: INSERT, Key: 2, Value: "value2"},
		{Operation: DELETE, Key: 3, Value: ""},
	},
}

var t2 = &SSTable[int32, string]{
	First: 4,
	Last:  9,
	Entries: []*Entry[int32, string]{
		{Operation: INSERT, Key: 4, Value: "value1"},
		{Operation: INSERT, Key: 5, Value: "value2"},
		{Operation: DELETE, Key: 9, Value: ""},
	},
}

func TestFindOverlappingTables(t *testing.T) {
	t.Run("Overlap one table", func(t *testing.T) {
		tbls := Overlapping(t1, []*SSTable[int32, string]{t1, t2})

		if len(tbls) != 1 {
			for _, table := range tbls {
				t.Logf("%v", table.Entries)
			}
			t.Errorf("Expected 1 table, found %v", len(tbls))
		}

		if !reflect.DeepEqual(t1, tbls[0]) {
			t.Error("Should be the same table")
		}
	})

	t.Run("Empty lower level", func(t *testing.T) {
		tbls := Overlapping(t1, []*SSTable[int32, string]{})
		if len(tbls) != 0 {
			t.Error("Should return 0 overlapping tables")
		}
	})

	t.Run("Overlap more than one table", func(t *testing.T) {
		wideTable := &SSTable[int32, string]{
			First: 1,
			Last:  5,
			Entries: []*Entry[int32, string]{
				{Operation: INSERT, Key: 1, Value: "value1"},
				{Operation: INSERT, Key: 2, Value: "value2"},
				{Operation: DELETE, Key: 3, Value: ""},
				{Operation: INSERT, Key: 4, Value: "value3"},
				{Operation: INSERT, Key: 5, Value: "value4"},
			},
		}

		tbls := Overlapping(wideTable, []*SSTable[int32, string]{t1, t2})
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

		oldest := Oldest(tables)

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
		_ = Oldest(tables)
	})

	t.Run("one element slice", func(t *testing.T) {
		tables := []*SSTable[int64, string]{{CreatedOn: time.Now()}}
		oldest := Oldest(tables)
		expectedOldest := tables[0]
		if !reflect.DeepEqual(oldest, expectedOldest) {
			t.Error("oldest element of 1-length slice should return element at index 0")
		}
	})
}

// TODO
func TestMerge(t *testing.T) {
}

// TODO
func TestSplit(t *testing.T) {
}

func TestGenerateUniqueSegmentName(t *testing.T) {
}
