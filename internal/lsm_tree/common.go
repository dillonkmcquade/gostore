package lsm_tree

import "cmp"

// A smallest-to-largest Node iterator
type Iterator[K cmp.Ordered, V any] interface {
	HasNext() bool
	Next() *Node[K, V]
}

// Iterable specifies a struct that may return an Iterator
type Iterable[K cmp.Ordered, V any] interface {
	Iterator() Iterator[K, V]
}

type MemTable[K cmp.Ordered, V any] interface {
	Iterable[K, V]
	// Insert Node to memTable
	Put(K, V)
	// Get returns a value associated with the key
	Get(K) (V, bool)
	// Returns the number of nodes in the memtable
	Size() uint
	// Clear points root to nil and makes size = 0
	Clear()
}

type LSMTree[K cmp.Ordered, V any] interface {
	// Write the Key-Value pair to the memtable
	Write(K, V) error
	// Read the value from the given key. Will return error if value is not found.
	Read(K) (V, error)
	// Delete the key from the DB
	Delete(K) error
	// Release associated resources
	Close() error
	// For debugging/tests: Use instead of Close to remove created files and release resources
	Clean() error
	// Recreate the memtable from WAL
	Replay(string) error
}
