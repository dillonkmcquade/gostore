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

// Binary search the current level for the key, returns false value if not found
func (l *Level[K, V]) BinarySearch(key K) (V, bool) {
	// Return index of table that COULD contain key
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First <= key && l.Tables[i].Last >= key })
	if index < len(l.Tables) {
		return l.Tables[index].Search(key)
	}
	return SSTableEntry[K, V]{}.Value, false
}

func (l *Level[K, V]) Add(table *SSTable[K, V], size int64) {
	if len(l.Tables) == 0 {
		l.Tables = append(l.Tables, table)
		l.Size += size
		return
	}
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].Last <= table.First })
	l.Tables = insertAt(l.Tables, index, table)
	l.Size += size
}

func (l *Level[K, V]) Remove(table *SSTable[K, V]) {
	assert(len(l.Tables) > 0)

	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First >= table.First })
	l.Tables = remove(l.Tables, index)
	size, err := table.Size()
	if err != nil {
		panic(err)
	}
	l.Size -= size
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

func (man *Manifest[K, V]) Persist() error {
	assert(len(man) == NUM_LEVELS)
	assert(man != nil)
	file, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_RDWR, 0777)
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

func NewManifest[K cmp.Ordered, V any]() (*Manifest[K, V], error) {
	_, err := os.Stat(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			manifest := &Manifest[K, V]{}

			assert(len(manifest) == NUM_LEVELS)

			level0_max_size := int64(LEVEL0_MAX_SIZE * 1024 * 1024) // convert to bytes

			for levelNumber := 0; levelNumber < NUM_LEVELS; levelNumber++ {
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
	return loadManifest[K, V]()
}

func loadManifest[K cmp.Ordered, V any]() (*Manifest[K, V], error) {
	file, err := os.Open(manifestPath)
	if err != nil {
		return nil, err
	}
	manifest := &Manifest[K, V]{}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(manifest)
	return manifest, err
}
