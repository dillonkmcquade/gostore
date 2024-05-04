package lsm_tree

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSSTableOverlaps(t *testing.T) {
	t.Run("Should not overlap", func(t *testing.T) {
		t1 := &SSTable[int64, string]{
			First: 0,
			Last:  9,
			Entries: []*SSTableEntry[int64, string]{
				{Operation: INSERT, Key: 0, Value: "value1"},
				{Operation: INSERT, Key: 2, Value: "value2"},
				{Operation: DELETE, Key: 9, Value: ""},
			},
		}
		t2 := &SSTable[int64, string]{
			First: 10,
			Last:  19,
			Entries: []*SSTableEntry[int64, string]{
				{Operation: INSERT, Key: 10, Value: "value1"},
				{Operation: INSERT, Key: 12, Value: "value2"},
				{Operation: DELETE, Key: 19, Value: ""},
			},
		}
		if t1.Overlaps(t2) || t2.Overlaps(t1) {
			t.Errorf("Should not overlap")
		}
	})

	t.Run("T1 should overlap T2", func(t *testing.T) {
		t1 := &SSTable[int64, string]{
			First: 0,
			Last:  9,
			Entries: []*SSTableEntry[int64, string]{
				{Operation: INSERT, Key: 0, Value: "value1"},
				{Operation: INSERT, Key: 2, Value: "value2"},
				{Operation: DELETE, Key: 9, Value: ""},
			},
		}
		t2 := &SSTable[int64, string]{
			First: 8,
			Last:  19,
			Entries: []*SSTableEntry[int64, string]{
				{Operation: INSERT, Key: 8, Value: "value1"},
				{Operation: INSERT, Key: 12, Value: "value2"},
				{Operation: DELETE, Key: 19, Value: ""},
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
		t1 := &SSTable[int64, string]{
			First: 0,
			Last:  9,
		}
		t2 := &SSTable[int64, string]{
			First: 8,
			Last:  19,
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
		t1 := &SSTable[int64, string]{
			Entries:   entries,
			Name:      filename,
			First:     0,
			Last:      100,
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
		t1 := &SSTable[int64, string]{
			Entries: entries,
			Name:    filename,
			Filter: NewBloomFilter[int64](&BloomFilterOpts{
				Size: 1000,
				Path: tmp,
			}),
			First:     0,
			Last:      100,
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
		t1.Filter.bitset = nil
		err = t1.LoadFilter()
		if err != nil {
			t.Error(err)
		}
		err = t1.Open()
		if err != nil {
			t.Error(err)
		}
		defer t1.Close()

		val, found := t1.Search(0)
		if !found {
			t.Error("Failed to search after opening table")
		}
		if val != "TESTVALUE0" {
			t.Error("value should be TESTVALUE0")
		}

		val, found = t1.Search(5)
		if !found {
			t.Error("Failed to search after opening table")
		}
		if val != "TESTVALUE5" {
			t.Error("value should be TESTVALUE0")
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
	t1 := &SSTable[int64, string]{
		Entries:   entries,
		Name:      filename,
		First:     0,
		Last:      100,
		CreatedOn: time.Now(),
	}

	t.Run("Search keys in table", func(t *testing.T) {
		if v, found := t1.Search(3); !found || v != "TESTVALUE3" {
			t.Errorf("Should be in table %v", 3)
		}
		if v, found := t1.Search(0); !found || v != "TESTVALUE0" {
			t.Errorf("Should be in table %v", 0)
		}
	})

	t.Run("Search keys not in table", func(t *testing.T) {
		if _, found := t1.Search(2); found {
			t.Errorf("%v should not be in table", 3)
		}
		if _, found := t1.Search(6); found {
			t.Errorf("%v should not be in table", 6)
		}
	})
}

func BenchmarkSSTableSearch(b *testing.B) {
	tmp := b.TempDir()
	filename := filepath.Join(tmp, "loadtest")
	entries := testEntries()
	t1 := &SSTable[int64, string]{
		Entries:   entries,
		Name:      filename,
		First:     0,
		Last:      100,
		CreatedOn: time.Now(),
	}
	b.Run("Search keys in table", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t1.Search(2)
		}
	})
}

func testEntries() []*SSTableEntry[int64, string] {
	return []*SSTableEntry[int64, string]{
		{
			Operation: INSERT,
			Key:       0,
			Value:     "TESTVALUE0",
		},
		{
			Operation: INSERT,
			Key:       1,
			Value:     "TESTVALUE1",
		},
		{
			Operation: INSERT,
			Key:       3,
			Value:     "TESTVALUE3",
		},
		{
			Operation: INSERT,
			Key:       5,
			Value:     "TESTVALUE5",
		},
		{
			Operation: INSERT,
			Key:       100,
			Value:     "TESTVALUE100",
		},
	}
}
