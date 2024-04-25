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
		}
		t2 := &SSTable[int64, string]{
			First: 10,
			Last:  19,
		}
		if t1.Overlaps(t2) {
			t.Errorf("Should not overlap")
		}
	})

	t.Run("T1 should overlap T2", func(t *testing.T) {
		t1 := &SSTable[int64, string]{
			First: 0,
			Last:  11,
		}
		t2 := &SSTable[int64, string]{
			First: 10,
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
		entries := []*SSTableEntry[int64, string]{
			{
				Operation: INSERT,
				Key:       0,
				Value:     "TESTVALUE",
			},
		}
		t1 := &SSTable[int64, string]{
			Entries:   entries,
			Name:      filename,
			First:     0,
			Last:      9,
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
		entries := []*SSTableEntry[int64, string]{
			{
				Operation: INSERT,
				Key:       0,
				Value:     "TESTVALUE",
			},
		}
		t1 := &SSTable[int64, string]{
			Entries:   entries,
			Name:      filename,
			First:     0,
			Last:      9,
			CreatedOn: time.Now(),
		}
		_, err := t1.Sync()
		if err != nil {
			t.Error(err)
		}

		err = t1.Open()
		defer t1.Close()
		if err != nil {
			t.Error(err)
		}
		if len(t1.Entries) != len(entries) {
			t.Error("Entries should not be empty")
		}
	})
}

func TestSSTableSearch(t *testing.T) {
	tmp := t.TempDir()
	filename := filepath.Join(tmp, "loadtest")
	entries := []*SSTableEntry[int64, string]{
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
	t1 := &SSTable[int64, string]{
		Entries:   entries,
		Name:      filename,
		First:     0,
		Last:      100,
		CreatedOn: time.Now(),
	}

	t.Run("Search keys in table", func(t *testing.T) {
		if _, found := t1.Search(3); !found {
			t.Errorf("Should be in table %v", 3)
		}
		if _, found := t1.Search(0); !found {
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
