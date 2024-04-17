package store

import (
	"cmp"
	"encoding/gob"
	"os"
	"sort"
)

// SSTableEntry represents an entry in the SSTable.
type SSTableEntry[K cmp.Ordered, V any] struct {
	Key   K
	Value V
}

// SSTable represents a Sorted String Table. Entries are sorted by key.
type SSTable[K cmp.Ordered, V any] struct {
	Entries []*SSTableEntry[K, V]
}

// WriteSSTable writes the contents of a memtable to an SSTable file.
func WriteSSTable[K cmp.Ordered, V any](tree MemTable[K, V], filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	iter := tree.Iterator()
	for iter.HasNext() {
		node := iter.Next()
		entry := &SSTableEntry[K, V]{Key: node.Key, Value: node.Value}
		if err := encoder.Encode(entry); err != nil {
			return err
		}
	}
	return nil
}

// ReadSSTable reads an SSTable file and returns its contents as an SSTable.
func ReadSSTable[K cmp.Ordered, V any](filename string) (*SSTable[K, V], error) {
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
	return &SSTable[K, V]{Entries: entries}, nil
}

// Search searches for a key in the SSTable.
func (table *SSTable[K, V]) Search(key K) (V, bool) {
	i := sort.Search(len(table.Entries), func(i int) bool { return table.Entries[i].Key >= key })
	if i < len(table.Entries) && table.Entries[i].Key == key {
		return table.Entries[i].Value, true
	}
	return table.Entries[i].Value, false
}
