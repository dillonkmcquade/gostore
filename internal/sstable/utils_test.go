package sstable

import (
	"reflect"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/filter"
)

var t1 = &SSTable[int32, string]{
	First: 1,
	Last:  3,
	Entries: []*Entry[int32, string]{
		{Operation: INSERT, Key: 1, Value: "value1"},
		{Operation: INSERT, Key: 2, Value: "value2"},
		{Operation: INSERT, Key: 3, Value: "value3"},
	},
}

var t2 = &SSTable[int32, string]{
	First: 4,
	Last:  9,
	Entries: []*Entry[int32, string]{
		{Operation: INSERT, Key: 4, Value: "value4"},
		{Operation: INSERT, Key: 5, Value: "value5"},
		{Operation: INSERT, Key: 9, Value: "value9"},
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
	total := 0
	for _, tbl := range []*SSTable[int32, string]{t1, t2} {
		total += len(tbl.Entries)
	}

	merged := Merge(t1, t2)

	if len(merged.Entries) != total {
		t.Errorf("Should have %v entries, found %v", total, len(merged.Entries))
	}
}

// TODO
func TestSplit(t *testing.T) {
	tmp := t.TempDir()
	tbl := &SSTable[int32, string]{
		First: 4,
		Last:  99,
		Entries: []*Entry[int32, string]{
			{Operation: INSERT, Key: 4, Value: "value4"},
			{Operation: INSERT, Key: 5, Value: "value5"},
			{Operation: INSERT, Key: 19, Value: "value9"},
			{Operation: INSERT, Key: 29, Value: "value9"},
			{Operation: INSERT, Key: 39, Value: "value9"},
			{Operation: INSERT, Key: 49, Value: "value9"},
		},
	}
	split := Split(tbl, 2, &Opts[int32, string]{
		BloomOpts: &filter.Opts{
			Size: 100,
			Path: tmp,
		},
		DestDir: tmp,
	})

	if len(split) != 3 {
		t.Errorf("Should have 3 tables, found %v", len(split))
	}
}

func TestGenerateUniqueSegmentName(t *testing.T) {
	timestamp := time.Now()
	name := GenerateUniqueSegmentName(timestamp)
	for i := 0; i < 1000; i++ {
		timestamp2 := time.Now()
		name2 := GenerateUniqueSegmentName(timestamp2)
		if name2 == name {
			t.Error("Should be different")
		}
	}
}
