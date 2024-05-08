package sstable

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/ordered"
)

type Operation byte

const (
	INSERT Operation = iota
	DELETE
)

// Entry represents an entry in the SSTable.
type Entry[K cmp.Ordered, V any] struct {
	Operation Operation
	Key       K
	Value     V
}

func (t Entry[K, V]) String() string {
	return fmt.Sprintf("{%v,%v}", t.Key, t.Value)
}

func (e *Entry[K, V]) Apply(c interface{}) {
	rbt := c.(*ordered.RedBlackTree[K, *Entry[K, V]])
	if e.Operation == INSERT {
		rbt.Put(e.Key, e)
	}
}

type Opts[K cmp.Ordered, V any] struct {
	BloomOpts *filter.Opts
	DestDir   string
	Entries   []*Entry[K, V]
}

func New[K cmp.Ordered, V any](opts *Opts[K, V]) *SSTable[K, V] {
	timestamp := time.Now()
	return &SSTable[K, V]{
		Name:      filepath.Join(opts.DestDir, GenerateUniqueSegmentName(timestamp)),
		Entries:   opts.Entries,
		Filter:    filter.New[K](opts.BloomOpts),
		CreatedOn: timestamp,
	}
}

// SSTable represents a Sorted String Table. Entries are sorted by key.
type SSTable[K cmp.Ordered, V any] struct {
	Entries   []*Entry[K, V]         // A list of entries sorted by key
	Filter    *filter.BloomFilter[K] // Check if key could be in table
	file      *os.File               // pointer to file descriptor for the table
	Size      int64                  // Size of file in bytes
	Name      string                 // full filename
	First     K                      // First key in range
	Last      K                      // Last key in range
	CreatedOn time.Time              // Timestamp
}

// Test if table key range overlaps the key range of another
func (table *SSTable[K, V]) Overlaps(anotherTable *SSTable[K, V]) bool {
	return (table.First >= anotherTable.First && table.First <= anotherTable.Last) ||
		(table.Last >= anotherTable.First && table.Last <= anotherTable.Last)
}

func (table *SSTable[K, V]) writeTo(writer io.Writer) (int64, error) {
	encoder := gob.NewEncoder(writer)
	err := encoder.Encode(table.Entries)
	return 0, err
}

func (table *SSTable[K, V]) getFile() (*os.File, error) {
	if table.file != nil {
		return table.file, nil
	}
	var err error
	table.file, err = os.OpenFile(table.Name, os.O_RDWR|os.O_CREATE, 0600)
	return table.file, err
}

// Sync flushes all in-memory entries to stable storage
func (table *SSTable[K, V]) Sync() (int64, error) {
	fd, err := table.getFile()
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	_, err = table.writeTo(fd)
	if err != nil {
		return 0, err
	}
	err = fd.Sync()
	if err != nil {
		return 0, err
	}
	table.clearEntries()
	err = table.updateSize()
	return table.Size, err
}

func (table *SSTable[K, V]) updateSize() error {
	fd, err := table.file.Stat()
	if err != nil {
		return err
	}
	size := fd.Size()
	table.Size = size
	return nil
}

func (table *SSTable[K, V]) clearEntries() {
	table.Entries = []*Entry[K, V]{}
}

func (table *SSTable[K, V]) SaveFilter() error {
	return table.Filter.Save()
}

func (table *SSTable[K, V]) LoadFilter() error {
	return table.Filter.Load()
}

// Read entries into memory & locks table
//
// *** You must call Close() after opening table
func (table *SSTable[K, V]) Open() error {
	if len(table.Entries) > 0 {
		// slog.Warn("Table entries should be empty before calling open")
		return nil
	}
	var err error
	table.file, err = os.Open(table.Name)
	if err != nil {
		return fmt.Errorf("os.OpenFile: %w", err)
	}
	return gob.NewDecoder(table.file).Decode(&table.Entries)
}

// Clears entries, unlocks table, and closes file
//
// Should only be called after prior call to Open()
func (table *SSTable[K, V]) Close() error {
	table.clearEntries()
	err := table.file.Close()
	if err != nil {
		return fmt.Errorf("file.Close: %w", err)
	}
	return nil
}

// Search searches for a key in the SSTable.
//
// Panics if attempt to search empty entries array
func (table *SSTable[K, V]) Search(key K) (V, bool) {
	assert.True(len(table.Entries) > 0, "Cannot search 0 entries")

	idx, found := sort.Find(len(table.Entries), func(i int) int { return cmp.Compare(key, table.Entries[i].Key) })
	if found {
		return table.Entries[idx].Value, true
	}
	return Entry[K, V]{}.Value, false
}
