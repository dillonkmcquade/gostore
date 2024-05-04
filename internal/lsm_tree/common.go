package lsm_tree

import (
	"cmp"
	"errors"
)

// TODO: Possible to extract this to config file
const (
	BLOOM_SIZE       = 200        // Size of Bloom filter bitset
	NUM_HASH_FUNCS   = 2          // Number of Hash Functions
	SSTABLE_MAX_SIZE = 40_000_000 // 40mb
	NUM_LEVELS       = 4          // Number of compaction levels
	LEVEL0_MAX_SIZE  = 300        // Max size of level0 in MB
)

var (
	ErrNotFound = errors.New("not found")
	ErrFileIO   = errors.New("error opening table")
)

type LSMTree[K cmp.Ordered, V any] interface {
	// Write the Key-Value pair to the memtable
	Write(K, V) error
	// Read the value from the given key. Will return error if value is not found.
	Read(K) (V, error)
	// Delete the key from the DB
	Delete(K) error
	// Release associated resources
	Close() error
}

// A smallest-to-largest Node iterator
type Iterator[K cmp.Ordered, V any] interface {
	HasNext() bool
	Next() *Node[K, V]
}

// Iterable specifies a struct that may return an Iterator
type Iterable[K cmp.Ordered, V any] interface {
	Iterator() Iterator[K, V]
}

// A key-value balanced tree data structure
type TreeMap[K cmp.Ordered, V any] interface {
	Iterable[K, V]
	Get(K) (V, bool)
	Put(K, V)
	Delete(K)
	Clear()
	Size() uint
}

// In-memory balanced key-value store
type MemTable[K cmp.Ordered, V any] interface {
	Put(K, V) error                 // Insert Node to memTable
	Get(K) (V, bool)                // Get returns a value associated with the key
	Delete(K)                       // Insert a node marked as delete
	ExceedsSize() bool              // Should memtable be flushed
	Snapshot(string) *SSTable[K, V] // Create snapshot of memtable as SSTable
	Clear()                         // Clear points root to nil and makes size = 0
	Close() error                   // Closes active resources
}

type CompactionController[K cmp.Ordered, V any] interface {
	Compact(*Manifest[K, V])
	Trigger(*Level[K, V]) bool
}
