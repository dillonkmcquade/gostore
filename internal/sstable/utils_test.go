package sstable

import (
	"reflect"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/pb"
)

var t1 = &SSTable{
	First: []byte{1},
	Last:  []byte{3},
	Entries: []*pb.SSTable_Entry{
		{Op: pb.Operation_INSERT, Key: []byte{1}, Value: []byte("value1")},
		{Op: pb.Operation_INSERT, Key: []byte{2}, Value: []byte("value2")},
		{Op: pb.Operation_INSERT, Key: []byte{3}, Value: []byte("value3")},
	},
}

var t2 = &SSTable{
	First: []byte{4},
	Last:  []byte{9},
	Entries: []*pb.SSTable_Entry{
		{Op: pb.Operation_INSERT, Key: []byte{4}, Value: []byte("value4")},
		{Op: pb.Operation_INSERT, Key: []byte{5}, Value: []byte("value5")},
		{Op: pb.Operation_INSERT, Key: []byte{9}, Value: []byte("value9")},
	},
}

func TestFindOverlappingTables(t *testing.T) {
	t.Run("Overlap one table", func(t *testing.T) {
		tbls := Overlapping(t1, []*SSTable{t1, t2})

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
		tbls := Overlapping(t1, []*SSTable{})
		if len(tbls) != 0 {
			t.Error("Should return 0 overlapping tables")
		}
	})

	t.Run("Overlap more than one table", func(t *testing.T) {
		wideTable := &SSTable{
			First: []byte{1},
			Last:  []byte{5},
			Entries: []*pb.SSTable_Entry{
				{Op: pb.Operation_INSERT, Key: []byte{1}, Value: []byte("value1")},
				{Op: pb.Operation_INSERT, Key: []byte{2}, Value: []byte("value2")},
				{Op: pb.Operation_DELETE, Key: []byte{3}, Value: []byte("")},
				{Op: pb.Operation_INSERT, Key: []byte{4}, Value: []byte("value3")},
				{Op: pb.Operation_INSERT, Key: []byte{5}, Value: []byte("value4")},
			},
		}

		tbls := Overlapping(wideTable, []*SSTable{t1, t2})
		if len(tbls) != 2 {
			t.Error("Should overlap two tables")
		}
	})
}

func TestFindOldestTable(t *testing.T) {
	t.Run("4 element slice", func(t *testing.T) {
		now := time.Now()
		tables := []*SSTable{
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
		tables := []*SSTable{}
		_ = Oldest(tables)
	})

	t.Run("one element slice", func(t *testing.T) {
		tables := []*SSTable{{CreatedOn: time.Now()}}
		oldest := Oldest(tables)
		expectedOldest := tables[0]
		if !reflect.DeepEqual(oldest, expectedOldest) {
			t.Error("oldest element of 1-length slice should return element at index 0")
		}
	})
}

func TestMerge(t *testing.T) {
	total := 0
	for _, tbl := range []*SSTable{t1, t2} {
		total += len(tbl.Entries)
	}

	merged := Merge(t1, t2)

	if len(merged.Entries) != total {
		t.Errorf("Should have %v entries, found %v", total, len(merged.Entries))
	}
}

func TestSplit(t *testing.T) {
	tmp := t.TempDir()
	tbl := &SSTable{
		First: []byte{4},
		Last:  []byte{99},
		Entries: []*pb.SSTable_Entry{
			{Op: pb.Operation_INSERT, Key: []byte{4}, Value: []byte("value4")},
			{Op: pb.Operation_INSERT, Key: []byte{5}, Value: []byte("value5")},
			{Op: pb.Operation_INSERT, Key: []byte{19}, Value: []byte("value9")},
			{Op: pb.Operation_INSERT, Key: []byte{29}, Value: []byte("value9")},
			{Op: pb.Operation_INSERT, Key: []byte{39}, Value: []byte("value9")},
			{Op: pb.Operation_INSERT, Key: []byte{49}, Value: []byte("value9")},
		},
	}
	split := Split(tbl, 2, &Opts{
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
