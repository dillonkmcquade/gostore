package lsm_tree

import (
	"cmp"
	"encoding/gob"
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
	CreatedOn time.Time // Timestamp

	mut sync.Mutex
}

func (table *SSTable[K, V]) Overlaps(anotherTable *SSTable[K, V]) bool {
	return (table.First >= anotherTable.First && table.First <= anotherTable.Last) ||
		(table.Last >= anotherTable.First && table.Last <= anotherTable.Last)
}

// Sync flushes all in-memory entries to stable storage
func (table *SSTable[K, V]) Sync() (int64, error) {
	_, err := table.Open()
	if err != nil {
		return 0, err
	}
	defer table.Close()
	encoder := gob.NewEncoder(table.file)
	err = encoder.Encode(table.Entries)
	if err != nil {
		return 0, err
	}
	table.file.Sync()
	clear(table.Entries)
	return table.Size()
}

// Closes file
func (table *SSTable[K, V]) Close() error {
	return table.file.Close()
}

// Opens file
func (table *SSTable[K, V]) Open() (*SSTable[K, V], error) {
	file, err := os.OpenFile(table.Name, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	table.file = file
	return table, nil
}

// Returns the file size in bytes
func (table *SSTable[K, V]) Size() (int64, error) {
	if table.file != nil {
		fd, err := table.file.Stat()
		if err != nil {
			return 0, err
		}
		return fd.Size(), nil
	}
	_, err := table.Open()
	defer table.Close()
	fd, err := table.file.Stat()
	if err != nil {
		return 0, err
	}
	return fd.Size(), nil
}

// Load entries into memory
// ** Must call Clear() after using Load to clear entries and unlock access to the table
func (table *SSTable[K, V]) Load() error {
	table.mut.Lock()
	if len(table.Entries) != 0 {
		return nil
	}
	_, err := table.Open()
	if err != nil {
		return err
	}
	defer table.Close()
	decoder := gob.NewDecoder(table.file)
	return decoder.Decode(&table.Entries)
}

// Clear table entries
func (table *SSTable[K, V]) Clear() {
	clear(table.Entries)
	table.mut.Unlock()
}

// Search searches for a key in the SSTable.
// Panics if attempt to search empty entries array
func (table *SSTable[K, V]) Search(key K) (V, bool) {
	if len(table.Entries) == 0 {
		panic("Cannot search empty SSTable")
	}
	idx, found := sort.Find(len(table.Entries), func(i int) int { return cmp.Compare(key, table.Entries[i].Key) })
	if found {
		return table.Entries[idx].Value, true
	}
	return SSTableEntry[K, V]{}.Value, false
}

// DEPRECATED
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
