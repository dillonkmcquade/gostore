package lsm_tree

import (
	"cmp"
	"encoding/json"
	"math"
	"os"
	"sort"
)

type Level[K cmp.Ordered, V any] struct {
	Number  int
	Tables  []*SSTable[K, V]
	Size    int64
	MaxSize int64
}

// Binary search the current level for table that has range overlapping key
func (l *Level[K, V]) BinarySearch(key K) (int, bool) {
	// find index of table that COULD contain key
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First <= key && l.Tables[i].Last >= key })
	if index >= 0 && index < len(l.Tables) {
		return index, true
	}
	return -1, false
}

func (l *Level[K, V]) Add(table *SSTable[K, V]) {
	if len(l.Tables) == 0 {
		l.Tables = append(l.Tables, table)
		l.Size += table.Size
		return
	}
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First <= table.First && l.Tables[i].Last <= table.Last })
	l.Tables = insertAt(l.Tables, index, table)
	l.Size += table.Size
}

func (l *Level[K, V]) Remove(table *SSTable[K, V]) {
	assert(len(l.Tables) > 0)

	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First <= table.First && l.Tables[i].Last <= table.Last })
	assert(index < len(l.Tables))
	if index < len(l.Tables) && l.Tables[index] == table {
		l.Tables = remove(l.Tables, index)
		l.Size -= table.Size
	}
}

func remove[T any](slice []T, i int) []T {
	return append(slice[:i], slice[i+1:]...)
}

func insertAt[T any](slice []T, i int, val T) []T {
	if i >= len(slice) {
		return append(slice, val)
	}
	slice = append(slice[:i+1], slice[i:]...)
	slice[i] = val
	return slice
}

// type Manifest[K cmp.Ordered, V any] [NUM_LEVELS]*Level[K, V]

type Manifest[K cmp.Ordered, V any] struct {
	Levels []*Level[K, V]
	Path   string
}

// Writes manifest to p. If p is nil, writes to manifestPath
func (man *Manifest[K, V]) Persist() error {
	assert(len(man.Levels) == NUM_LEVELS)
	assert(man != nil)

	file, err := os.OpenFile(man.Path, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	err = file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(man.Levels)
}

type ManifestOpts struct {
	Path            string // Path to manifest
	Num_levels      int    // Number of compaction levels
	Level0_max_size int64  // Max size of level 0 in bytes
}

// Create new manifest
func NewManifest[K cmp.Ordered, V any](opts *ManifestOpts) (*Manifest[K, V], error) {
	_, err := os.Stat(opts.Path)
	if err != nil {
		if os.IsNotExist(err) {
			manifest := &Manifest[K, V]{
				Path:   opts.Path,
				Levels: make([]*Level[K, V], opts.Num_levels),
			}

			assert(len(manifest.Levels) == opts.Num_levels)

			for levelNumber := 0; levelNumber < opts.Num_levels; levelNumber++ {
				multiplier := math.Pow(10, float64(levelNumber))
				manifest.Levels[levelNumber] = &Level[K, V]{
					Number:  levelNumber,
					Size:    0,
					MaxSize: opts.Level0_max_size * int64(multiplier),
				}
			}
			return manifest, nil
		}
		return nil, err
	}
	return loadManifest[K, V](opts.Path)
}

func loadManifest[K cmp.Ordered, V any](p string) (*Manifest[K, V], error) {
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	manifest := &Manifest[K, V]{}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&manifest.Levels)
	return manifest, err
}
