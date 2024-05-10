package manifest

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/pb"
	"github.com/dillonkmcquade/gostore/internal/sstable"
)

func newManifest(t *testing.T) (*Manifest, error) {
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
	return New(opts)
}

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
	man, err := New(opts)
	if err != nil {
		t.Error(err)
	}
	defer man.Close()
}

func TestManifestAddTable(t *testing.T) {
	man, err := newManifest(t)
	if err != nil {
		t.Error(err)
	}
	t2 := &sstable.SSTable{
		First:     []byte{10},
		Last:      []byte{19},
		Name:      filepath.Join(t.TempDir(), "test_segment.segment"),
		Size:      100,
		CreatedOn: time.Now(),
		Entries: []*pb.SSTable_Entry{
			{Op: pb.Operation_INSERT, Key: []byte{10}, Value: []byte("value1")},
			{Op: pb.Operation_INSERT, Key: []byte{12}, Value: []byte("value2")},
			{Op: pb.Operation_DELETE, Key: []byte{19}, Value: []byte("")},
		},
	}

	err = man.AddTable(t2, 0)
	if err != nil {
		t.Errorf("man.AddTable: %v", err.Error())
	}

	if len(man.Levels[0].Tables) != 1 {
		t.Error("Level zero should be of length 1")
	}
	if man.Levels[0].Size != 100 {
		t.Error("Size should be 100")
	}
}

func TestManifestRemoveTable(t *testing.T) {
	man, err := newManifest(t)
	if err != nil {
		t.Error(err)
	}
	t2 := &sstable.SSTable{
		First:     []byte{10},
		Last:      []byte{19},
		Name:      filepath.Join(t.TempDir(), "test_segment.segment"),
		Size:      100,
		CreatedOn: time.Now(),
		Entries: []*pb.SSTable_Entry{
			{Op: pb.Operation_INSERT, Key: []byte{10}, Value: []byte("value1")},
			{Op: pb.Operation_INSERT, Key: []byte{12}, Value: []byte("value2")},
			{Op: pb.Operation_DELETE, Key: []byte{19}, Value: []byte("")},
		},
	}

	err = man.AddTable(t2, 0)
	if err != nil {
		t.Errorf("man.AddTable: %v", err.Error())
	}

	err = man.RemoveTable(t2, 0)
	if err != nil {
		t.Errorf("man.AddTable: %v", err.Error())
	}

	if man.Levels[0].Size != 0 || len(man.Levels[0].Tables) != 0 {
		t.Error("Expected size 0 and table length 0")
	}
}

func TestManifest_ClearLevel(t *testing.T) {
	man, err := newManifest(t)
	if err != nil {
		t.Error(err)
	}
	t2 := &sstable.SSTable{
		First:     []byte{10},
		Last:      []byte{19},
		Name:      filepath.Join(t.TempDir(), "test_segment.segment"),
		Size:      100,
		CreatedOn: time.Now(),
		Entries: []*pb.SSTable_Entry{
			{Op: pb.Operation_INSERT, Key: []byte{10}, Value: []byte("value1")},
			{Op: pb.Operation_INSERT, Key: []byte{12}, Value: []byte("value2")},
			{Op: pb.Operation_DELETE, Key: []byte{19}, Value: []byte("")},
		},
	}

	err = man.AddTable(t2, 0)
	if err != nil {
		t.Errorf("man.AddTable: %v", err.Error())
	}
	err = man.ClearLevel(0)
	if man.Levels[0].Size != 0 || len(man.Levels[0].Tables) != 0 {
		t.Error("Expected size 0 and table length 0")
	}
}

// TODO
func TestManifestReplay(t *testing.T) {
}

func newTestManifest(path string) *Manifest {
	return &Manifest{
		Levels: []*Level{
			{
				Number: 0,
				Tables: []*sstable.SSTable{
					{
						Entries: []*pb.SSTable_Entry{
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{0},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{1},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{2},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{3},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{4},
								Value: []byte("TEST"),
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
						First:     []byte{0},
						Last:      []byte{4},
						Size:      0,
						CreatedOn: time.Now(),
					},
					{
						Entries: []*pb.SSTable_Entry{
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{50},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{55},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{60},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{70},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{80},
								Value: []byte("TEST"),
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
						First:     []byte{50},
						Last:      []byte{80},
						Size:      0,
						CreatedOn: time.Now(),
					},
				},
				Size:    0,
				MaxSize: 0,
			},
			{
				Number: 1,
				Tables: []*sstable.SSTable{
					{
						Entries: []*pb.SSTable_Entry{
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{7},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{8},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{9},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{10},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{14},
								Value: []byte("TEST"),
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
						First:     []byte{7},
						Last:      []byte{14},
						Size:      0,
						CreatedOn: time.Now(),
					},
					{
						Entries: []*pb.SSTable_Entry{
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{21},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{28},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{29},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{31},
								Value: []byte("TEST"),
							},
							{
								Op:    pb.Operation_INSERT,
								Key:   []byte{40},
								Value: []byte("TEST"),
							},
						},
						Name:      filepath.Join(path, sstable.GenerateUniqueSegmentName(time.Now())),
						First:     []byte{21},
						Last:      []byte{40},
						Size:      0,
						CreatedOn: time.Now(),
					},
				},
				Size:    0,
				MaxSize: 0,
			},
			{
				Number: 2,
				Tables: []*sstable.SSTable{{
					Entries:   []*pb.SSTable_Entry{},
					Name:      "",
					Size:      0,
					CreatedOn: time.Now(),
				}},
				Size:    0,
				MaxSize: 0,
			},
			{
				Number: 3,
				Tables: []*sstable.SSTable{{
					Entries:   []*pb.SSTable_Entry{},
					Name:      "",
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
