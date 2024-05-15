package sstable

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/pb"
)

func TestSSTableOverlaps(t *testing.T) {
	t.Run("Should not overlap", func(t *testing.T) {
		t1 := &SSTable{
			First: []byte{0},
			Last:  []byte{9},
			Entries: []*pb.SSTable_Entry{
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{0}, Value: []byte("value1")},
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{2}, Value: []byte("value2")},
				{Op: pb.Operation_OPERATION_DELETE, Key: []byte{9}, Value: []byte("")},
			},
		}
		t2 := &SSTable{
			First: []byte{10},
			Last:  []byte{19},
			Entries: []*pb.SSTable_Entry{
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{10}, Value: []byte("value1")},
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{12}, Value: []byte("value2")},
				{Op: pb.Operation_OPERATION_DELETE, Key: []byte{19}, Value: []byte("")},
			},
		}
		if t1.Overlaps(t2) || t2.Overlaps(t1) {
			t.Errorf("Should not overlap")
		}
	})

	t.Run("T1 should overlap T2", func(t *testing.T) {
		t1 := &SSTable{
			First: []byte{0},
			Last:  []byte{9},
			Entries: []*pb.SSTable_Entry{
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{0}, Value: []byte("value1")},
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{2}, Value: []byte("value2")},
				{Op: pb.Operation_OPERATION_DELETE, Key: []byte{9}, Value: []byte("")},
			},
		}
		t2 := &SSTable{
			First: []byte{8},
			Last:  []byte{19},
			Entries: []*pb.SSTable_Entry{
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{8}, Value: []byte("value1")},
				{Op: pb.Operation_OPERATION_INSERT, Key: []byte{12}, Value: []byte("value2")},
				{Op: pb.Operation_OPERATION_DELETE, Key: []byte{19}, Value: []byte("")},
			},
		}
		if !t1.Overlaps(t2) {
			t.Errorf("T1 should overlap T2")
		}
		if !t2.Overlaps(t1) {
			t.Error("T2 should overlap t1")
		}
	})

	t.Run("T1 should overlap T2- no entries", func(t *testing.T) {
		t1 := &SSTable{
			First: []byte{0},
			Last:  []byte{9},
		}
		t2 := &SSTable{
			First: []byte{8},
			Last:  []byte{19},
		}
		if !t1.Overlaps(t2) {
			t.Errorf("T1 should overlap T2")
		}
		if !t2.Overlaps(t1) {
			t.Error("T2 should overlap t1")
		}
	})
}

func TestSSTableIO(t *testing.T) {
	t.Run("Sync", func(t *testing.T) {
		tmp := t.TempDir()
		filename := filepath.Join(tmp, "synctest")
		entries := testEntries()
		t1 := &SSTable{
			Entries:   entries,
			Name:      filename,
			First:     []byte{0},
			Last:      []byte{100},
			CreatedOn: time.Now(),
		}
		size, err := t1.Sync()
		if err != nil {
			t.Error(err)
		}

		if size <= 0 {
			t.Error("Size should be non-zero")
		}

		if len(t1.Entries) > 0 {
			t.Error("Entries should have been cleared")
		}
	})

	t.Run("Open/Close", func(t *testing.T) {
		tmp := t.TempDir()
		filename := filepath.Join(tmp, "loadtest")
		entries := testEntries()
		t1 := &SSTable{
			Entries: entries,
			Name:    filename,
			Filter: filter.New(&filter.Opts{
				Size: 1000,
				Path: tmp,
			}),
			First:     []byte{0},
			Last:      []byte{100},
			CreatedOn: time.Now(),
		}
		for _, entry := range entries {
			t1.Filter.Add(entry.Key)
		}
		_, err := t1.Sync()
		if err != nil {
			t.Error(err)
		}
		err = t1.SaveFilter()
		if err != nil {
			t.Error(err)
		}
		t1.Filter.Clear()
		err = t1.LoadFilter()
		if err != nil {
			t.Error(err)
		}
		err = t1.Open()
		if err != nil {
			t.Error(err)
		}
		defer t1.Close()

		val, found := t1.Search(([]byte{0}))
		if !found {
			t.Error("Failed to search after opening table")
		}
		if !slices.Equal(val, []byte("TESTVALUE0")) {
			t.Error("value should be TESTVALUE0")
		}

		val, found = t1.Search(([]byte{5}))
		if !found {
			t.Error("Failed to search after opening table")
		}
		if slices.Compare(val, []byte("TESTVALUE5")) != 0 {
			t.Error("value should be TESTVALUE5")
		}

		if len(t1.Entries) != len(entries) {
			t.Error("Entries should not be empty")
		}
	})
}

func TestSSTableSearch(t *testing.T) {
	tmp := t.TempDir()
	filename := filepath.Join(tmp, "loadtest")
	entries := testEntries()
	t1 := &SSTable{
		Entries:   entries,
		Name:      filename,
		First:     []byte{0},
		Last:      []byte{100},
		CreatedOn: time.Now(),
	}

	t.Run("Search keys in table", func(t *testing.T) {
		if _, found := t1.Search([]byte{3}); !found {
			t.Errorf("Should be in table %v", 3)
		}
		if _, found := t1.Search(([]byte{0})); !found {
			t.Errorf("Should be in table %v", 0)
		}
	})

	t.Run("Search keys not in table", func(t *testing.T) {
		if _, found := t1.Search([]byte{33}); found {
			t.Errorf("%v should not be in table", 3)
		}
		if _, found := t1.Search([]byte{6}); found {
			t.Errorf("%v should not be in table", 6)
		}
	})
}

// func BenchmarkSSTableSearch(b *testing.B) {
// 	tmp := b.TempDir()
// 	filename := filepath.Join(tmp, "loadtest")
// 	entries := testEntries()
// 	t1 := &SSTable{
// 		Entries:   entries,
// 		Name:      filename,
// 		First:     []byte{0},
// 		Last:      []byte{100},
// 		CreatedOn: time.Now(),
// 	}
// 	b.Run("Search keys in table", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			t1.Search([]byte{2})
// 		}
// 	})
// }

func testEntries() []*pb.SSTable_Entry {
	return []*pb.SSTable_Entry{
		{
			Op:    pb.Operation_OPERATION_INSERT,
			Key:   []byte{0},
			Value: []byte("TESTVALUE0"),
		},
		{
			Op:    pb.Operation_OPERATION_INSERT,
			Key:   []byte{1},
			Value: []byte("TESTVALUE1"),
		},
		{
			Op:    pb.Operation_OPERATION_INSERT,
			Key:   []byte{3},
			Value: []byte("TESTVALUE3"),
		},
		{
			Op:    pb.Operation_OPERATION_INSERT,
			Key:   []byte{5},
			Value: []byte("TESTVALUE5"),
		},
		{
			Op:    pb.Operation_OPERATION_INSERT,
			Key:   []byte{100},
			Value: []byte("TESTVALUE100"),
		},
	}
}
