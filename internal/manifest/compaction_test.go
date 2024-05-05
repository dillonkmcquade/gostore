package manifest

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/sstable"
)

func newTestLevel(baseDir string, n int) *Level[int32, string] {
	l := &Level[int32, string]{
		Path:    filepath.Join(baseDir, fmt.Sprintf("l%v", n)),
		Number:  n,
		Size:    0,
		MaxSize: 300,
	}

	for i := 0; i < 10; i++ {
		t := sstable.New(&sstable.Opts[int32, string]{
			BloomOpts: &filter.Opts{
				Size: 100,
				Path: filepath.Join(baseDir, "filters"),
			},
			DestDir: l.Path,
		})

		for j := 0; j < 10; j++ {
			t.Entries = append(t.Entries, &sstable.Entry[int32, string]{Key: int32(rand.Intn(100)), Value: "TEST", Operation: sstable.INSERT})
		}
	}
	os.MkdirAll(l.Path, 0750)
	os.MkdirAll(filepath.Join(baseDir, "filters"), 0750)
	return l
}

func TestLevel0Compaction(t *testing.T) {
	tmp := t.TempDir()
	man, err := New[int32, string](&Opts{
		Path: filepath.Join(tmp, "manifest.json"),
		LevelPaths: []string{
			filepath.Join(tmp, "l0"), filepath.Join(tmp, "l1"), filepath.Join(tmp, "l2"), filepath.Join(tmp, "l3"),
		},
		Num_levels:       4,
		Level0_max_size:  500000,
		SSTable_max_size: 2,
		BloomPath:        filepath.Join(tmp, "filters"),
	})
	for _, p := range man.Levels {
		os.MkdirAll(p.Path, 0750)
	}
	os.MkdirAll(man.BloomPath, 0750)
	if err != nil {
		t.Error(err)
	}

	t1 := sstable.New(&sstable.Opts[int32, string]{
		BloomOpts: &filter.Opts{
			Size: 100,
			Path: man.BloomPath,
		},
		DestDir: man.Levels[0].Path,
		Entries: []*sstable.Entry[int32, string]{
			{
				Operation: sstable.INSERT,
				Key:       0,
				Value:     "TEST",
			},
			{
				Operation: sstable.INSERT,
				Key:       1,
				Value:     "TEST",
			},
			{
				Operation: sstable.INSERT,
				Key:       2,
				Value:     "TEST",
			},
			{
				Operation: sstable.INSERT,
				Key:       3,
				Value:     "TEST",
			},
		},
	})
	t2 := sstable.New(&sstable.Opts[int32, string]{
		BloomOpts: &filter.Opts{
			Size: 100,
			Path: man.BloomPath,
		},
		DestDir: man.Levels[0].Path,
		Entries: []*sstable.Entry[int32, string]{
			{
				Operation: sstable.INSERT,
				Key:       5,
				Value:     "TEST",
			},
			{
				Operation: sstable.INSERT,
				Key:       6,
				Value:     "TEST",
			},
			{
				Operation: sstable.INSERT,
				Key:       7,
				Value:     "TEST",
			},
			{
				Operation: sstable.INSERT,
				Key:       8,
				Value:     "TEST",
			},
		},
	})
	man.AddTable(t1, 0)
	man.AddTable(t2, 0)

	t.Run("Level 0", func(t *testing.T) {
		man.level_0_compact(man.Levels[0])
		if len(man.Levels[1].Tables) != 4 {
			t.Error("Should be 4")
		}
		if man.Levels[1].Tables[0].First != 0 {
			t.Error("Should be 0")
		}
		if man.Levels[1].Tables[0].Last != 1 {
			t.Error("Should be 0")
		}
		if len(man.Levels[0].Tables) != 0 {
			t.Error("Level 0 should be empty after compaction")
		}
	})

	t.Run("Lower Level compact", func(t *testing.T) {
		man.lower_level_compact(man.Levels[1])
		if len(man.Levels[2].Tables) != 1 || len(man.Levels[1].Tables) != 3 {
			t.Errorf("Level 2 should contain 1 table, found %v. Level 1 should contain 3 tables, found %v", len(man.Levels[2].Tables), len(man.Levels[1].Tables))
		}
	})
}
