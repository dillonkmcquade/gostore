package lsm_tree

import (
	"path/filepath"
	"testing"
	"time"
)

func newTestManifest(path string) *Manifest[int64, string] {
	return &Manifest[int64, string]{
		Levels: []*Level[int64, string]{
			{
				Number: 0,
				Tables: []*SSTable[int64, string]{
					{
						Entries: []*SSTableEntry[int64, string]{
							{
								Operation: INSERT,
								Key:       0,
								Value:     "TEST",
							},
							{
								Operation: INSERT,
								Key:       1,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       2,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       3,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       4,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, generateUniqueSegmentName(time.Now())),
						First:     0,
						Last:      4,
						Size:      0,
						CreatedOn: time.Now(),
					},
					{
						Entries: []*SSTableEntry[int64, string]{
							{
								Operation: INSERT,
								Key:       50,
								Value:     "TEST",
							},
							{
								Operation: INSERT,
								Key:       100,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       200,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       300,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       400,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, generateUniqueSegmentName(time.Now())),
						First:     50,
						Last:      400,
						Size:      0,
						CreatedOn: time.Now(),
					},
				},
				Size:    0,
				MaxSize: 0,
			},
			{
				Number: 1,
				Tables: []*SSTable[int64, string]{
					{
						Entries: []*SSTableEntry[int64, string]{
							{
								Operation: INSERT,
								Key:       7,
								Value:     "TEST",
							},
							{
								Operation: INSERT,
								Key:       8,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       9,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       10,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       14,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, generateUniqueSegmentName(time.Now())),
						First:     7,
						Last:      14,
						Size:      0,
						CreatedOn: time.Now(),
					},
					{
						Entries: []*SSTableEntry[int64, string]{
							{
								Operation: INSERT,
								Key:       21,
								Value:     "TEST",
							},
							{
								Operation: INSERT,
								Key:       28,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       29,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       31,
								Value:     "",
							},
							{
								Operation: INSERT,
								Key:       40,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, generateUniqueSegmentName(time.Now())),
						First:     7,
						Last:      14,
						Size:      0,
						CreatedOn: time.Now(),
					},
				},
				Size:    0,
				MaxSize: 0,
			},
			{
				Number: 2,
				Tables: []*SSTable[int64, string]{{
					Entries: []*SSTableEntry[int64, string]{
						{
							Operation: INSERT,
							Key:       40,
							Value:     "TEST",
						},
						{
							Operation: INSERT,
							Key:       1,
							Value:     "",
						},
						{
							Operation: INSERT,
							Key:       2,
							Value:     "",
						},
						{
							Operation: INSERT,
							Key:       3,
							Value:     "",
						},
						{
							Operation: INSERT,
							Key:       4,
							Value:     "",
						},
					},
					Name:      "",
					First:     0,
					Last:      0,
					Size:      0,
					CreatedOn: time.Now(),
				}},
				Size:    0,
				MaxSize: 0,
			},
			{
				Number: 3,
				Tables: []*SSTable[int64, string]{{
					Entries: []*SSTableEntry[int64, string]{
						{
							Operation: INSERT,
							Key:       0,
							Value:     "TEST",
						},
						{
							Operation: INSERT,
							Key:       1,
							Value:     "",
						},
						{
							Operation: INSERT,
							Key:       2,
							Value:     "",
						},
						{
							Operation: INSERT,
							Key:       3,
							Value:     "",
						},
						{
							Operation: INSERT,
							Key:       4,
							Value:     "",
						},
					},
					Name:      "",
					First:     0,
					Last:      0,
					Size:      0,
					CreatedOn: time.Now(),
				}},
				Size:    0,
				MaxSize: 0,
			},
		},
		Path: filepath.Join(path, "manifest.json"),
	}
}

func TestNewManifest(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "manifest.json")
	opts := &ManifestOpts{
		Path:            path,
		Num_levels:      NUM_LEVELS,
		Level0_max_size: LEVEL0_MAX_SIZE,
	}
	man, err := NewManifest[int64, string](opts)
	if err != nil {
		t.Error(err)
	}
	defer man.Close()
}

// func TestManifestPersist(t *testing.T) {
// 	tmp := t.TempDir()
// 	path := filepath.Join(tmp, "manifest.json")
// 	opts := &ManifestOpts{
// 		Path:            path,
// 		Num_levels:      NUM_LEVELS,
// 		Level0_max_size: LEVEL0_MAX_SIZE,
// 	}
// 	man, err := NewManifest[int64, string](opts)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	defer man.Close()
// 	man.AddTable()
//
// }

func TestLevelBinarySearch(t *testing.T) {
	// path := "../../data/sortedManifest.json"
	// opts := &ManifestOpts{Path: path, Num_levels: NUM_LEVELS, Level0_max_size: LEVEL0_MAX_SIZE}
	// man, err := NewManifest[int64, string](opts)
	// if err != nil {
	// 	t.Error(err)
	// }
	tmp := t.TempDir()
	man := newTestManifest(tmp)

	_, found := man.Levels[0].BinarySearch(0)
	if !found {
		t.Error("Should be in Level")
	}
	// if found {
	// 	err = man.Levels[0].Tables[i].Open()
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// 	if _, found := man.Levels[0].Tables[i].Search(0); !found {
	// 		t.Error("Should contain 0")
	// 	}
	// 	man.Levels[0].Tables[i].Close()
	// }

	_, found = man.Levels[0].BinarySearch(400)
	if !found {
		t.Error("Should be in Level")
	}
	// if found {
	// 	err = man.Levels[0].Tables[i].Open()
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// 	if _, found := man.Levels[0].Tables[i].Search(400); !found {
	// 		t.Error("Should contain 400")
	// 	}
	// 	man.Levels[0].Tables[i].Close()
	// }
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
	t1 := &SSTable[int64, string]{
		First: 0,
		Last:  10,
		Size:  10,
	}

	t2 := &SSTable[int64, string]{
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
