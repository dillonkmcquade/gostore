package lsm_tree

import (
	"path/filepath"
	"testing"
	"time"
)

func TestNewManifest(t *testing.T) {
	defer CleanAppFiles()
	_, err := NewManifest[int64, string](nil)
	if err != nil {
		t.Error(err)
	}
}

func TestManifestPersist(t *testing.T) {
	man, err := NewManifest[int64, string](nil)
	if err != nil {
		t.Error(err)
	}

	tmp := t.TempDir()
	manFile := filepath.Join(tmp, "manifest.json")
	err = man.Persist(&manFile)
	if err != nil {
		t.Error(err)
	}

	_, err = loadManifest[int64, string](manFile)
	if err != nil {
		t.Error(err)
	}
}

func TestLevelAdd(t *testing.T) {
	level := &Level[int64, string]{
		Number:  0,
		Tables:  []*SSTable[int64, string]{},
		Size:    100,
		MaxSize: 200,
	}

	for i := 100; i > 0; i -= 10 {
		level.Add(&SSTable[int64, string]{
			Entries:   []*SSTableEntry[int64, string]{},
			file:      nil,
			Name:      "test",
			First:     int64(i),
			Last:      int64(i - 9),
			CreatedOn: time.Now(),
		}, 10)
	}

	for i := 0; i < len(level.Tables)-1; i++ {
		if level.Tables[i].Overlaps(level.Tables[i+1]) {
			t.Errorf("%v should not overlap %v", level.Tables[i], level.Tables[i+1])
		}
	}

	if len(level.Tables) != 10 {
		t.Error("Should be 10")
	}
	if level.Size != 200 {
		t.Error("Should be 200")
	}
}

func TestLevelRemove(t *testing.T) {
	level := &Level[int64, string]{
		Number:  0,
		Size:    0,
		MaxSize: 200,
	}
	t1 := &SSTable[int64, string]{
		First: 0,
		Last:  10,
	}

	t2 := &SSTable[int64, string]{
		First: 11,
		Last:  20,
	}

	level.Add(t1, 10)
	level.Add(t2, 10)

	if level.Size != 20 {
		t.Error("Should be 20")
	}

	if len(level.Tables) != 2 {
		t.Error("Length should be 2")
	}

	level.Remove(t1, 10)

	if len(level.Tables) != 1 {
		t.Error("Length should be 1")
	}
	if level.Size != 10 {
		t.Error("Size should be 10")
	}
	level.Remove(t2, 10)
	if len(level.Tables) != 0 {
		t.Error("Length should be 0")
	}
	if level.Size != 0 {
		t.Error("Size should be 0")
	}
}

// func TestManifestAdd(t *testing.T) {}
// func TestManifestAdd(t *testing.T) {}
// func TestManifestAdd(t *testing.T) {}
