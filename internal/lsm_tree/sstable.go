package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

// SSTableEntry represents an entry in the SSTable.
type SSTableEntry[K cmp.Ordered, V any] struct {
	Operation Operation
	Key       K
	Value     V
}

func (t SSTableEntry[K, V]) String() string {
	return fmt.Sprintf("{%v,%v}", t.Key, t.Value)
}

type NewTableOpts[K cmp.Ordered, V any] struct {
	BloomOpts *BloomFilterOpts
	Name      string
	Entries   []*SSTableEntry[K, V]
}

func NewSSTable[K cmp.Ordered, V any](opts *NewTableOpts[K, V]) *SSTable[K, V] {
	return &SSTable[K, V]{
		Name:      opts.Name,
		Entries:   opts.Entries,
		Filter:    NewBloomFilter[K](opts.BloomOpts),
		CreatedOn: time.Now(),
	}
}

// SSTable represents a Sorted String Table. Entries are sorted by key.
type SSTable[K cmp.Ordered, V any] struct {
	Entries   []*SSTableEntry[K, V]
	Filter    *BloomFilter[K]
	file      *os.File
	Size      int64     // Size of file in bytes
	Name      string    // full filename
	First     K         // First key in range
	Last      K         // Last key in range
	CreatedOn time.Time // Timestamp

	mut sync.Mutex
}

// Test if table key range overlaps the key range of another
func (table *SSTable[K, V]) Overlaps(anotherTable *SSTable[K, V]) bool {
	return (table.First >= anotherTable.First && table.First <= anotherTable.Last) ||
		(table.Last >= anotherTable.First && table.Last <= anotherTable.Last)
}

// Sync flushes all in-memory entries to stable storage
func (table *SSTable[K, V]) Sync() (int64, error) {
	tableFile, err := os.OpenFile(table.Name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return 0, err
	}
	defer tableFile.Close()

	encoder := gob.NewEncoder(tableFile)
	err = encoder.Encode(table.Entries)
	if err != nil {
		return 0, err
	}
	err = tableFile.Sync()
	if err != nil {
		return 0, err
	}
	table.Entries = []*SSTableEntry[K, V]{}
	table.file = tableFile
	fd, err := tableFile.Stat()
	if err != nil {
		return 0, err
	}
	size := fd.Size()
	table.Size = size

	return size, err
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
		slog.Warn("Table entries should be empty before calling open")
		return nil
	}
	file, err := os.OpenFile(table.Name, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	table.file = file
	decoder := gob.NewDecoder(file)
	return decoder.Decode(&table.Entries)
}

// Clears entries, unlocks table, and closes file
//
// Should only be called after prior call to Open()
func (table *SSTable[K, V]) Close() error {
	table.Entries = []*SSTableEntry[K, V]{}
	return table.file.Close()
}

// Search searches for a key in the SSTable.
//
// Panics if attempt to search empty entries array
func (table *SSTable[K, V]) Search(key K) (V, bool) {
	assert(len(table.Entries) > 0, "Cannot search 0 entries")

	idx, found := sort.Find(len(table.Entries), func(i int) int { return cmp.Compare(key, table.Entries[i].Key) })
	if found {
		return table.Entries[idx].Value, true
	}
	return SSTableEntry[K, V]{}.Value, false
}
