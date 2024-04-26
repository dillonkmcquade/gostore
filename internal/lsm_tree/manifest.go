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

func (l *Level[K, V]) Add(table *SSTable[K, V], size int64) {
	if len(l.Tables) == 0 {
		l.Tables = append(l.Tables, table)
		l.Size += size
		return
	}
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First <= table.First && l.Tables[i].Last <= table.Last })
	l.Tables = insertAt(l.Tables, index, table)
	l.Size += size
}

func (l *Level[K, V]) Remove(table *SSTable[K, V], size int64) {
	assert(len(l.Tables) > 0)

	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First <= table.First && l.Tables[i].Last <= table.Last })
	assert(index < len(l.Tables))
	if index < len(l.Tables) && l.Tables[index] == table {
		l.Tables = remove(l.Tables, index)
		l.Size -= size
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

type Manifest[K cmp.Ordered, V any] [NUM_LEVELS]*Level[K, V]

// Writes manifest to p. If p is nil, writes to manifestPath
func (man *Manifest[K, V]) Persist(p *string) error {
	assert(len(man) == NUM_LEVELS)
	assert(man != nil)

	var path string
	if p == nil {
		path = manifestPath
	} else {
		path = *p
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0777)
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
	return encoder.Encode(man)
}

type ManifestOpts struct {
	path            string // Path to manifest
	num_levels      int    // Number of compaction levels
	level0_max_size int64  // Size of level 0
}

// Create new manifest
func NewManifest[K cmp.Ordered, V any](opts *ManifestOpts) (*Manifest[K, V], error) {
	_, err := os.Stat(opts.path)
	if err != nil {
		if os.IsNotExist(err) {
			manifest := &Manifest[K, V]{}

			assert(len(manifest) == opts.num_levels)

			level0_max_size := opts.level0_max_size * 1024 * 1024 // convert to bytes

			for levelNumber := 0; levelNumber < opts.num_levels; levelNumber++ {
				multiplier := math.Pow(10, float64(levelNumber))
				manifest[levelNumber] = &Level[K, V]{
					Number:  levelNumber,
					Size:    0,
					MaxSize: level0_max_size * int64(multiplier),
				}
			}
			return manifest, nil
		}
		return nil, err
	}
	return loadManifest[K, V](opts.path)
}

func loadManifest[K cmp.Ordered, V any](p string) (*Manifest[K, V], error) {
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	manifest := &Manifest[K, V]{}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(manifest)
	return manifest, err
}
