package lsm_tree

import (
	"cmp"
	"encoding/gob"
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

// SSTable represents a Sorted String Table. Entries are sorted by key.
type SSTable[K cmp.Ordered, V any] struct {
	Entries   []*SSTableEntry[K, V]
	file      *os.File
	Name      string    // full filename
	First     K         // First key in range
	Last      K         // Last key in range
	Size      int64     // Size of file in bytes
	CreatedOn time.Time // Timestamp

	mut sync.Mutex
}

func (table *SSTable[K, V]) Overlaps(anotherTable *SSTable[K, V]) bool {
	return (table.First >= anotherTable.First && table.First <= anotherTable.Last) ||
		(table.Last >= anotherTable.First && table.Last <= anotherTable.Last)
}

// Sync flushes all in-memory entries to stable storage
func (table *SSTable[K, V]) Sync() (int64, error) {
	file, err := os.OpenFile(table.Name, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return 0, err
	}
	slog.Info("File Creation", "type", "SSTable", "name", table.Name)
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(table.Entries)
	if err != nil {
		return 0, err
	}
	err = file.Sync()
	if err != nil {
		return 0, err
	}
	table.Entries = []*SSTableEntry[K, V]{}
	table.file = file

	fd, err := file.Stat()
	if err != nil {
		return 0, err
	}

	size := fd.Size()

	table.Size = size

	return size, nil
}

// Read entries into memory & locks table
//
// *** You must call Close() after opening table
func (table *SSTable[K, V]) Open() error {
	assert(len(table.Entries) == 0)
	table.mut.Lock()
	file, err := os.OpenFile(table.Name, os.O_RDONLY, 0777)
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
	table.mut.Unlock()
	return table.file.Close()
}

// Search searches for a key in the SSTable.
//
// Panics if attempt to search empty entries array
func (table *SSTable[K, V]) Search(key K) (V, bool) {
	assert(len(table.Entries) > 0)

	idx, found := sort.Find(len(table.Entries), func(i int) int { return cmp.Compare(key, table.Entries[i].Key) })
	if found {
		return table.Entries[idx].Value, true
	}
	return SSTableEntry[K, V]{}.Value, false
}

// DEPRECATED
//
// readSSTableFromFile reads an SSTable file and returns its contents as an SSTable.
func readSSTableFromFile[K cmp.Ordered, V any](filename string) (*SSTable[K, V], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []*SSTableEntry[K, V]
	decoder := gob.NewDecoder(file)
	for {
		var entry SSTableEntry[K, V]
		if err := decoder.Decode(&entry); err != nil {
			break
		}
		entries = append(entries, &entry)
	}
	return &SSTable[K, V]{Entries: entries, Name: filename, file: file}, err
}
