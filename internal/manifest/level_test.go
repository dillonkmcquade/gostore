package manifest

import (
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/sstable"
)

func TestLevelBinarySearch(t *testing.T) {
	tmp := t.TempDir()
	man := newTestManifest(tmp)

	_, found := man.Levels[0].BinarySearch(0)
	if !found {
		t.Error("Should be in Level")
	}

	_, found = man.Levels[0].BinarySearch(400)
	if !found {
		t.Error("Should be in Level")
	}
}

func TestLevelAdd(t *testing.T) {
	level := &Level[int64, string]{
		Number:  0,
		Tables:  []*sstable.SSTable[int64, string]{},
		Size:    100,
		MaxSize: 200,
	}

	for i := 100; i > 0; i -= 10 {
		level.Add(&sstable.SSTable[int64, string]{
			Entries:   []*sstable.Entry[int64, string]{},
			Name:      "test",
			First:     int64(i),
			Last:      int64(i - 9),
			CreatedOn: time.Now(),
		})
	}

	for i := 0; i < len(level.Tables)-1; i++ {
		if level.Tables[i].Overlaps(level.Tables[i+1]) {
			t.Errorf("%v should not overlap %v", level.Tables[i], level.Tables[i+1])
		}
	}

	if len(level.Tables) != 10 {
		t.Error("Should be 10")
	}
}

func TestLevelRemove(t *testing.T) {
	level := &Level[int64, string]{
		Number:  0,
		Size:    0,
		MaxSize: 200,
	}
	t1 := &sstable.SSTable[int64, string]{
		First: 0,
		Last:  10,
		Size:  10,
	}

	t2 := &sstable.SSTable[int64, string]{
		First: 11,
		Last:  20,
		Size:  10,
	}

	level.Add(t1)
	level.Add(t2)

	if level.Size != 20 {
		t.Error("Should be 20")
	}

	if len(level.Tables) != 2 {
		t.Error("Length should be 2")
	}

	level.Remove(t1)

	if len(level.Tables) != 1 {
		t.Error("Length should be 1")
	}
	if level.Size != 10 {
		t.Error("Size should be 10")
	}
	level.Remove(t2)
	if len(level.Tables) != 0 {
		t.Error("Length should be 0")
	}
	if level.Size != 0 {
		t.Error("Size should be 0")
	}
}
