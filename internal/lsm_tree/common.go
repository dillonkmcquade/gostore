package lsm_tree

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

// In-memory balanced key-value store
type MemTable[K cmp.Ordered, V any] interface {
	Iterable[K, V]
	// Insert Node to memTable
	Put(K, V) error
	// Get returns a value associated with the key
	Get(K) (V, bool)
	// Insert a node marked as delete
	Delete(K)
	// Returns the number of nodes in the memtable
	Size() uint
	// Should memtable be flushed
	ExceedsSize() bool
	// Create snapshot of memtable as SSTable
	Snapshot(string) *SSTable[K, V]

	// Generate new empty memtable with the same options
	Clone() MemTable[K, V]
	// Clear points root to nil and makes size = 0
	Clear()
	// Closes active resources
	Close() error
}

type CompactionController[K cmp.Ordered, V any] interface {
	Compact(*Manifest[K, V])
	// GenerateCompactionTask(Manifest[K, V]) *CompactionTask[K, V]
	Trigger(*Level[K, V]) bool
}

// Creates directory if it does not exist.
func mkDir(filename string) error {
	slog.Debug("File Creation", "type", "dir", "name", filename)
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return os.MkdirAll(filename, 0750)
	}
	return err
}

// Panics if statement does not resolve to true
func assert(stmt bool) {
	if !stmt {
		panic(fmt.Sprintf("Failed assert: %v", stmt))
	}
}

func remove[T any](slice []T, i int) []T {
	return append(slice[:i], slice[i+1:]...)
}

func insertAt[T any](slice []T, i int, val T) []T {
	if i >= len(slice) {
		return append(slice, val)
	}
	slice = append(slice[:i+1], slice[i:]...)
	slice[i] = val
	return slice
}
