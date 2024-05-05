package manifest

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/sstable"
)

func TestNewManifest(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "manifest.json")
	opts := &Opts{
		Path: path,
		LevelPaths: []string{
			filepath.Join(tmp, "l0"), filepath.Join(tmp, "l1"), filepath.Join(tmp, "l2"), filepath.Join(tmp, "l3"),
		},
		Num_levels:       4,
		Level0_max_size:  500000,
		SSTable_max_size: 1000,
		BloomPath:        filepath.Join(tmp, "filters"),
	}
	man, err := New[int64, string](opts)
	if err != nil {
		t.Error(err)
	}
	defer man.Close()
}

// TODO
func TestManifestAddTable(t *testing.T) {
}

// TODO
func TestManifestRemoveTable(t *testing.T) {
}

// TODO
func TestManifestClearLevel(t *testing.T) {
}

// TODO
func TestManifestReplay(t *testing.T) {
}

func newTestManifest(path string) *Manifest[int64, string] {
	return &Manifest[int64, string]{
		Levels: []*Level[int64, string]{
			{
				Number: 0,
				Tables: []*sstable.SSTable[int64, string]{
					{
						Entries: []*sstable.Entry[int64, string]{
							{
								Operation: sstable.INSERT,
								Key:       0,
								Value:     "TEST",
							},
							{
								Operation: sstable.INSERT,
								Key:       1,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       2,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       3,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       4,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
						First:     0,
						Last:      4,
						Size:      0,
						CreatedOn: time.Now(),
					},
					{
						Entries: []*sstable.Entry[int64, string]{
							{
								Operation: sstable.INSERT,
								Key:       50,
								Value:     "TEST",
							},
							{
								Operation: sstable.INSERT,
								Key:       100,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       200,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       300,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       400,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
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
				Tables: []*sstable.SSTable[int64, string]{
					{
						Entries: []*sstable.Entry[int64, string]{
							{
								Operation: sstable.INSERT,
								Key:       7,
								Value:     "TEST",
							},
							{
								Operation: sstable.INSERT,
								Key:       8,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       9,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       10,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       14,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
						First:     7,
						Last:      14,
						Size:      0,
						CreatedOn: time.Now(),
					},
					{
						Entries: []*sstable.Entry[int64, string]{
							{
								Operation: sstable.INSERT,
								Key:       21,
								Value:     "TEST",
							},
							{
								Operation: sstable.INSERT,
								Key:       28,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       29,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       31,
								Value:     "",
							},
							{
								Operation: sstable.INSERT,
								Key:       40,
								Value:     "",
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
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
				Tables: []*sstable.SSTable[int64, string]{{
					Entries: []*sstable.Entry[int64, string]{
						{
							Operation: sstable.INSERT,
							Key:       40,
							Value:     "TEST",
						},
						{
							Operation: sstable.INSERT,
							Key:       1,
							Value:     "",
						},
						{
							Operation: sstable.INSERT,
							Key:       2,
							Value:     "",
						},
						{
							Operation: sstable.INSERT,
							Key:       3,
							Value:     "",
						},
						{
							Operation: sstable.INSERT,
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
				Tables: []*sstable.SSTable[int64, string]{{
					Entries: []*sstable.Entry[int64, string]{
						{
							Operation: sstable.INSERT,
							Key:       0,
							Value:     "TEST",
						},
						{
							Operation: sstable.INSERT,
							Key:       1,
							Value:     "",
						},
						{
							Operation: sstable.INSERT,
							Key:       2,
							Value:     "",
						},
						{
							Operation: sstable.INSERT,
							Key:       3,
							Value:     "",
						},
						{
							Operation: sstable.INSERT,
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
