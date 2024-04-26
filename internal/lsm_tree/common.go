package lsm_tree

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	userHome     = os.Getenv("HOME")
	gostorePath  = filepath.Join(userHome, ".gostore")                   // Base data directory
	level0       = filepath.Join(gostorePath, "l0")                      // Contains level-0 SSTables
	level1       = filepath.Join(gostorePath, "l1")                      // Contains level-1 SSTables
	level2       = filepath.Join(gostorePath, "l2")                      // Contains level-2 SSTables
	level3       = filepath.Join(gostorePath, "l3")                      // Contains level-3 SSTables
	manifestPath = filepath.Join(gostorePath, "manifest.json")           // Information about levels
	walPath      = filepath.Join(gostorePath, "wal.dat")                 // Path to WAL
	bloomPath    = filepath.Join(gostorePath, "bloomfilter.dat")         // Path to saved bloom filter
	appDirs      = []string{gostorePath, level0, level1, level2, level3} // Important application directories
)

// A number-to-directory path mapping used to retrieve the proper directory for saving new segment files
//
//	0: /home/$USER/.gostore/l0
//	1: /home/$USER/.gostore/l1
//	etc...
var numberToPathMap = map[int]string{
	0: level0,
	1: level1,
	2: level2,
	3: level3,
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
	// Reads memtable entries into SSTable
	Snapshot(string) *SSTable[K, V]
	// Clear points root to nil and makes size = 0
	Clear()
	// Closes active resources
	Close() error
}

type CompactionController[K cmp.Ordered, V any] interface {
	Compact(*CompactionTask[K, V], *Manifest[K, V]) error
	// GenerateCompactionTask(Manifest[K, V]) *CompactionTask[K, V]
	Trigger(int, *Manifest[K, V]) *CompactionTask[K, V]
}

// Creates directory if it does not exist.
func mkDir(filename string) error {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return os.Mkdir(filename, 0777)
	}
	return err
}

// FOR TESTING: Clean up applicationo files:
//
//	Segment files
//	WAL file
//	Manifest file
//	Bloom File
func CleanAppFiles() error {
	// Remove all segment files from level directories
	for _, levelDir := range numberToPathMap {
		segments, err := os.ReadDir(levelDir)
		if err != nil {
			return err
		}
		for _, segment := range segments {
			if strings.HasSuffix(segment.Name(), ".segment") {
				err = os.Remove(filepath.Join(levelDir, segment.Name()))
				if err != nil {
					return err
				}
			}
		}
	}
	os.Remove(bloomPath)
	os.Remove(manifestPath)
	return os.Remove(walPath)
}

// Panics if statement does not resolve to true
func assert(stmt bool) {
	if !stmt {
		panic(fmt.Sprintf("Failed assert: %v", stmt))
	}
}
