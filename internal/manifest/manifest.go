package manifest

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/sstable"
)

type ManifestOp byte

const (
	ADDTABLE ManifestOp = iota
	REMOVETABLE
	CLEARTABLE
)

type ManifestEntry[K cmp.Ordered, V any] struct {
	Op    ManifestOp
	Level int
	Table *sstable.SSTable[K, V]
}

func (entry *ManifestEntry[K, V]) Apply(level *Level[K, V]) {
	switch entry.Op {
	case ADDTABLE:
		level.Add(entry.Table)
	case REMOVETABLE:
		level.Remove(entry.Table)
	case CLEARTABLE:
		level.Clear()
	}
}

type Manifest[K cmp.Ordered, V any] struct {
	Levels           []*Level[K, V] // in-memory representation of levels
	Path             string         // path to manifest
	encoder          *gob.Encoder
	file             *os.File
	SSTable_max_size int
	BloomPath        string

	crudmut sync.Mutex
	compmut sync.Mutex
}

type Opts struct {
	Path             string   // Path to manifest
	LevelPaths       []string // Path to each level directory
	Num_levels       int      // Number of compaction levels
	Level0_max_size  int64    // Max size of level 0 in bytes
	SSTable_max_size int
	BloomPath        string
}

// Create new manifest
func New[K cmp.Ordered, V any](opts *Opts) (*Manifest[K, V], error) {
	var manifest *Manifest[K, V]
	file, err := os.OpenFile(opts.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}
	manifest = &Manifest[K, V]{
		Path:             opts.Path,
		Levels:           make([]*Level[K, V], opts.Num_levels),
		encoder:          gob.NewEncoder(file),
		file:             file,
		SSTable_max_size: opts.SSTable_max_size,
		BloomPath:        opts.BloomPath,
	}
	for levelNumber := 0; levelNumber < opts.Num_levels; levelNumber++ {
		multiplier := math.Pow(10, float64(levelNumber))
		manifest.Levels[levelNumber] = &Level[K, V]{
			Number:  levelNumber,
			Size:    0,
			MaxSize: opts.Level0_max_size * int64(multiplier),
			Path:    opts.LevelPaths[levelNumber],
		}
	}
	err = manifest.Replay()
	if err != nil {
		return nil, fmt.Errorf("manifest.Replay: %w", err)
	}
	return manifest, nil
}

func (m *Manifest[K, V]) AddTable(table *sstable.SSTable[K, V], level int) error {
	m.crudmut.Lock()
	m.Levels[level].Add(table)
	entry := &ManifestEntry[K, V]{Op: ADDTABLE, Table: table, Level: level}
	err := m.encoder.Encode(entry)
	if err != nil {
		return fmt.Errorf("encoder.Encode: %w", err)
	}
	err = m.file.Sync()
	if err != nil {
		return fmt.Errorf("file.Sync: %w", err)
	}
	m.crudmut.Unlock()
	return nil
}

func (m *Manifest[K, V]) RemoveTable(table *sstable.SSTable[K, V], level int) error {
	m.crudmut.Lock()
	defer m.crudmut.Unlock()
	m.Levels[level].Remove(table)
	entry := &ManifestEntry[K, V]{Op: REMOVETABLE, Table: table, Level: level}
	err := m.encoder.Encode(entry)
	if err != nil {
		return fmt.Errorf("encoder.Encode: %w", err)
	}
	return m.file.Sync()
}

func (m *Manifest[K, V]) ClearLevel(level int) error {
	m.crudmut.Lock()
	defer m.crudmut.Unlock()
	m.Levels[level].Clear()
	entry := &ManifestEntry[K, V]{Op: CLEARTABLE, Table: nil, Level: level}
	err := m.encoder.Encode(entry)
	if err != nil {
		return fmt.Errorf("encoder.Encode: %w", err)
	}
	return m.file.Sync()
}

func (m *Manifest[K, V]) Close() error {
	return m.file.Close()
}

func (m *Manifest[K, V]) Replay() error {
	file, err := os.Open(m.Path)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("os.Open: %w", err)
	}
	decoder := gob.NewDecoder(file)

	for {
		var entry ManifestEntry[K, V]
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decoder.Decode: %w", err)
		}
		entry.Apply(m.Levels[entry.Level])
	}
	for _, level := range m.Levels {
		for _, tbl := range level.Tables {
			err = tbl.LoadFilter()
			if err != nil {
				slog.Error("Replay: error loading filter")
				panic(err)
			}
		}
	}
	return nil
}

// remove element at index i from slice
func remove[T any](slice []T, i int) []T {
	return append(slice[:i], slice[i+1:]...)
}

// insert val into slice at index i
func insertAt[T any](slice []T, i int, val T) []T {
	if i >= len(slice) {
		return append(slice, val)
	}
	slice = append(slice[:i+1], slice[i:]...)
	slice[i] = val
	return slice
}
