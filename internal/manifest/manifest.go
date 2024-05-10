package manifest

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	"github.com/dillonkmcquade/gostore/internal/ordered"
	"github.com/dillonkmcquade/gostore/internal/sstable"
	"github.com/dillonkmcquade/gostore/internal/wal"
)

type ManifestOp byte

const (
	ADDTABLE ManifestOp = iota
	REMOVETABLE
	CLEARTABLE
)

type ManifestEntry struct {
	Op    ManifestOp
	Level int
	Table *sstable.SSTable
}

func (entry *ManifestEntry) Apply(c interface{}) {
	level := c.(*Level)
	switch entry.Op {
	case ADDTABLE:
		level.Add(entry.Table)
	case REMOVETABLE:
		level.Remove(entry.Table)
	case CLEARTABLE:
		level.Clear()
	}
}

type Manifest struct {
	Levels            []*Level                 // in-memory representation of levels
	wal               *wal.WAL[*ManifestEntry] // Manifest log
	Path              string                   // path to manifest
	SSTable_max_size  int                      // Max size to use when splitting tables
	BloomPath         string                   // Path to filters directory
	waitForCompaction sync.WaitGroup           // finish compaction before exiting
	compactionTicker  *time.Ticker             // Check if levels need compaction on an interval
	mut               sync.RWMutex
	done              chan bool
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
func New(opts *Opts) (*Manifest, error) {
	var manifest *Manifest
	wal, err := wal.New[*ManifestEntry](opts.Path, 1)
	if err != nil {
		return nil, err
	}
	manifest = &Manifest{
		Path:             opts.Path,
		wal:              wal,
		Levels:           make([]*Level, opts.Num_levels),
		SSTable_max_size: opts.SSTable_max_size,
		BloomPath:        opts.BloomPath,
		compactionTicker: time.NewTicker(2 * time.Second),
		done:             make(chan bool, 1),
	}
	for levelNumber := 0; levelNumber < opts.Num_levels; levelNumber++ {
		multiplier := math.Pow(10, float64(levelNumber))
		manifest.Levels[levelNumber] = &Level{
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
	go manifest.Compact()
	return manifest, nil
}

var ErrNotFound = errors.New("Not found")

func (m *Manifest) Search(key []byte) ([]byte, error) {
	var errs []error

	if v, err := m.searchL0(key); err != nil {
		errs = append(errs, fmt.Errorf("level 0 search error: %w", err))
	} else {
		return v, nil
	}

	if v, err := m.searchLowerLevels(key); err != nil {
		errs = append(errs, fmt.Errorf("lower level search error: %w", err))
	} else {
		return v, nil
	}
	return []byte{}, errors.Join(errs...)
}

func (m *Manifest) searchL0(key []byte) ([]byte, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	level0 := m.Levels[0]
	for i := len(level0.Tables) - 1; i >= 0; i-- {
		tbl := level0.Tables[i]

		if tbl.Filter.Has(key) {
			err := tbl.Open()
			if err != nil {
				slog.Error("Read: error opening table", "filename", tbl.Name)
				slog.Error(err.Error())
				return ordered.Node[[]byte, []byte]{}.Value, err
			}

			if val, found := tbl.Search(key); found {
				return val, nil
			}
			err = tbl.Close()
			if err != nil {
				slog.Error(err.Error())
				return []byte{}, fmt.Errorf("tbl.Close: %w", err)
			}
		}
	}
	return []byte{}, ErrNotFound
}

func (m *Manifest) searchLowerLevels(key []byte) ([]byte, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	// binary search sorted levels 1:3 sequentially
	for _, level := range m.Levels[1:] {
		if i, found := level.BinarySearch(key); found {
			if level.Tables[i].Filter.Has(key) {
				err := level.Tables[i].Open()
				if err != nil {
					slog.Error("Read: error opening table", "filename", level.Tables[i].Name)
					slog.Error(err.Error())
					return ordered.Node[[]byte, []byte]{}.Value, fmt.Errorf("tbl.Open: %w", err)
				}
				defer level.Tables[i].Close()
				if val, found := level.Tables[i].Search(key); found {
					return val, nil
				}
			}
		}
	}
	return []byte{}, ErrNotFound
}

func (m *Manifest) AddTable(table *sstable.SSTable, level int) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	m.Levels[level].Add(table)
	slog.Debug("Adding table to level", "level", level, "size", m.Levels[level].Size, "maxSize", m.Levels[level].MaxSize, "Should flush", m.Levels[level].Size > m.Levels[level].MaxSize)
	entry := &ManifestEntry{Op: ADDTABLE, Table: table, Level: level}
	err := m.wal.Write(entry)
	if err != nil {
		return fmt.Errorf("wal.Write: %w", err)
	}
	return nil
}

func (m *Manifest) RemoveTable(table *sstable.SSTable, level int) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	m.Levels[level].Remove(table)
	entry := &ManifestEntry{Op: REMOVETABLE, Table: table, Level: level}
	err := m.wal.Write(entry)
	if err != nil {
		return fmt.Errorf("encoder.Encode: %w", err)
	}
	return nil
}

func (m *Manifest) ClearLevel(level int) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	m.Levels[level].Clear()
	entry := &ManifestEntry{Op: CLEARTABLE, Table: nil, Level: level}
	err := m.wal.Write(entry)
	if err != nil {
		return fmt.Errorf("encoder.Encode: %w", err)
	}
	return nil
}

func (m *Manifest) Close() error {
	m.waitForCompaction.Wait()
	close(m.done)
	return nil
}

func (m *Manifest) Replay() error {
	file, err := os.Open(m.Path)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("os.Open: %w", err)
	}
	decoder := gob.NewDecoder(file)

	for {
		var entry ManifestEntry
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
