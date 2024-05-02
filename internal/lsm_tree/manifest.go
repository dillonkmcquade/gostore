package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
)

type Level[K cmp.Ordered, V any] struct {
	Number  int
	Tables  []*SSTable[K, V]
	Size    int64
	MaxSize int64
	mut     sync.Mutex
}

// Binary search the current level for table that has range overlapping key
func (l *Level[K, V]) BinarySearch(key K) (int, bool) {
	low := 0
	high := len(l.Tables) - 1

	for low <= high {
		mid := low + (high-low)/2
		if l.Tables[mid].First <= key && key <= l.Tables[mid].Last {
			return mid, true
		}

		if key < l.Tables[mid].First {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return -1, false
}

func (l *Level[K, V]) Add(table *SSTable[K, V]) {
	l.mut.Lock()
	defer l.mut.Unlock()
	if len(l.Tables) == 0 {
		l.Tables = append(l.Tables, table)
		l.Size += table.Size
		return
	}
	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First >= table.First && l.Tables[i].Last >= table.Last })
	l.Tables = insertAt(l.Tables, index, table)
	l.Size += table.Size
}

func (l *Level[K, V]) Remove(table *SSTable[K, V]) {
	l.mut.Lock()
	defer l.mut.Unlock()
	assert(len(l.Tables) > 0)

	index := sort.Search(len(l.Tables), func(i int) bool { return l.Tables[i].First >= table.First && l.Tables[i].Last >= table.Last })
	assert(index < len(l.Tables))
	if index < len(l.Tables) && l.Tables[index] == table {
		l.Tables = remove(l.Tables, index)
		l.Size -= table.Size
	}
}

// Assigns an empty array to Tables and sets size to 0
func (l *Level[K, V]) Clear() {
	l.mut.Lock()
	l.Tables = []*SSTable[K, V]{}
	l.Size = 0
	l.mut.Unlock()
}

type ManifestOp int

const (
	ADD ManifestOp = iota
	REMOVE
	CLEAR
)

type ManifestEntry[K cmp.Ordered, V any] struct {
	Op    ManifestOp
	Level int
	Table *SSTable[K, V]
}

func (entry *ManifestEntry[K, V]) Apply(level *Level[K, V]) {
	switch entry.Op {
	case ADD:
		level.Add(entry.Table)
	case REMOVE:
		level.Remove(entry.Table)
	case CLEAR:
		level.Clear()
	}
}

type Manifest[K cmp.Ordered, V any] struct {
	Levels  []*Level[K, V] // in-memory representation of levels
	Path    string         // path to manifest
	encoder *gob.Encoder
	file    *os.File
}

type ManifestOpts struct {
	Path            string // Path to manifest
	Num_levels      int    // Number of compaction levels
	Level0_max_size int64  // Max size of level 0 in bytes
}

// Create new manifest
func NewManifest[K cmp.Ordered, V any](opts *ManifestOpts) (*Manifest[K, V], error) {
	var manifest *Manifest[K, V]
	file, err := os.OpenFile(opts.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("NewManifest: %v", err)
	}
	manifest = &Manifest[K, V]{
		Path:    opts.Path,
		Levels:  make([]*Level[K, V], opts.Num_levels),
		encoder: gob.NewEncoder(file),
		file:    file,
	}
	for levelNumber := 0; levelNumber < opts.Num_levels; levelNumber++ {
		multiplier := math.Pow(10, float64(levelNumber))
		manifest.Levels[levelNumber] = &Level[K, V]{
			Number:  levelNumber,
			Size:    0,
			MaxSize: opts.Level0_max_size * int64(multiplier),
		}
	}
	err = manifest.Replay()
	if err != nil {
		return nil, fmt.Errorf("NewManifest: %v", err)
	}
	return manifest, nil
}

func (m *Manifest[K, V]) AddTable(table *SSTable[K, V], level int) error {
	m.Levels[level].Add(table)
	entry := &ManifestEntry[K, V]{Op: ADD, Table: table, Level: level}
	err := m.encoder.Encode(entry)
	if err != nil {
		return fmt.Errorf("AddTable: %v", err)
	}
	logFileIO[K, V](ENCODE, MANIFEST, entry)
	return m.file.Sync()
}

func (m *Manifest[K, V]) RemoveTable(table *SSTable[K, V], level int) error {
	m.Levels[level].Remove(table)
	entry := &ManifestEntry[K, V]{Op: REMOVE, Table: table, Level: level}
	err := m.encoder.Encode(entry)
	if err != nil {
		return fmt.Errorf("RemoveTable: %v", err)
	}
	logFileIO[K, V](ENCODE, MANIFEST, entry)
	return m.file.Sync()
}

func (m *Manifest[K, V]) ClearLevel(level int) error {
	m.Levels[level].Clear()
	entry := &ManifestEntry[K, V]{Op: CLEAR, Table: nil, Level: level}
	err := m.encoder.Encode(entry)
	if err != nil {
		return fmt.Errorf("ClearLevel: %v", err)
	}
	logFileIO[K, V](ENCODE, MANIFEST, entry)
	return m.file.Sync()
}

func (m *Manifest[K, V]) Close() error {
	return m.file.Close()
}

func (m *Manifest[K, V]) Replay() error {
	file, err := os.Open(m.Path)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("Manifest Replay: %v", err)
	}
	decoder := gob.NewDecoder(file)

	for {
		var entry ManifestEntry[K, V]
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("Manifest Replay: %v", err)
		}
		entry.Apply(m.Levels[entry.Level])
		logFileIO[K, V](DECODE, MANIFEST, entry)
	}
	for _, level := range m.Levels {
		for _, tbl := range level.Tables {
			err = tbl.LoadFilter()
			if err != nil {
				panic(err)
			}
		}
	}
	return nil
}
