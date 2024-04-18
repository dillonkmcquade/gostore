package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type GoStore[K cmp.Ordered, V any] struct {
	// The current memtable
	memTable MemTable[K, V]

	// Filenames of sstables ordered oldest to most recent
	segments []string

	// Verify if the key exists in the DB quickly
	bloom *BloomFilter[K]

	// The max size before the memtable is flushed to disk
	max_size uint

	// The Write-Ahead-Log (wal) contains a log of all in-memory operations
	// prior to flushing. If the database crashes with data in-memory that has not
	// been written to disk, the current in-memory state may be recreated again after restart.
	wal *WAL[K, V]

	mut sync.RWMutex
}

// Creates a new LSMTree. Creates a cache directory under the
// users XDG_CACHE_DIR to store data if it does not exist
func New[K cmp.Ordered, V any]() LSMTree[K, V] {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		panic("XDG_CACHE_DIR does not exist") // Directory must exist
	}

	gostorePath := filepath.Join(cacheDir, "gostore")

	_, err = os.Stat(gostorePath)
	if os.IsNotExist(err) {
		err = os.Mkdir(gostorePath, 0644)
		if err != nil {
			panic(err) // Directory must exist in order to store data files
		}
	}

	// TREE
	tree := newRedBlackTree[K, V]()

	// WAL
	wal, err := newWal[K, V](filepath.Join(gostorePath, "wal.db"))
	if err != nil {
		panic(err)
	}

	// BLOOMFILTER
	var bloom *BloomFilter[K]
	bloom, err = loadBloomFromFile[K](filepath.Join(gostorePath, "bloomfilter.dat"))
	if err != nil {
		bloom = NewBloomFilter[K](200000, 2)
	}

	return &GoStore[K, V]{memTable: tree, wal: wal, bloom: bloom}
}

// Iterate over segments from newest to oldest
type SSTableIterator struct {
	index    int
	segments []string
}

func (iter *SSTableIterator) HasNext() bool {
	return iter.index > 0
}

func (iter *SSTableIterator) Next() string {
	if iter.HasNext() {
		iter.index--
		segment := iter.segments[iter.index]
		return segment
	}
	return ""
}

// Returns a newest -> oldest segment iterator
func newSSTableIterator(segments *[]string) *SSTableIterator {
	return &SSTableIterator{index: len(*segments), segments: *segments}
}

// Write memTable to disk as SSTable
func (self *GoStore[K, V]) flush() {
	// Persist in-memory data
	cacheDir, err := os.UserCacheDir()
	table := filepath.Join(cacheDir, "gostore", fmt.Sprintf("%v.segment", time.Now().Unix()))
	err = writeSSTable(self.memTable, table)
	if err != nil {
		log.Fatalf("Unable to build SSTable : %v", err)
	}
	self.mut.Unlock()

	// Save filename for reads
	self.segments = append(self.segments, table)

	// Discard memTable
	self.memTable.Clear()

	// Discard write-ahead log
	self.wal.Discard()
}

func (self *GoStore[L, V]) exceeds_size() bool {
	return self.memTable.Size() > self.max_size
}

// Insert to memtable
func (self *GoStore[K, V]) Write(key K, val V) error {
	self.mut.Lock()

	// Write to memTable
	self.memTable.Put(key, val)
	self.bloom.Add(key)
	if self.exceeds_size() {
		go self.flush()
		return nil
	}
	self.mut.Unlock()
	return nil
}

func (self *GoStore[K, V]) Read(key K) (V, error) {
	self.mut.RLock()
	// Read from memory
	if val, ok := self.memTable.Get(key); ok {
		return val, nil
	} else {
		// Read from disk
		iter := newSSTableIterator(&self.segments)
		for iter.HasNext() {
			filename := iter.Next()
			table, err := readSSTable[K, V](filename)
			if err != nil {
				return Node[K, V]{}.Value, err
			}
			if val, ok := table.Search(key); ok {
				return val, nil
			} else {
				continue
			}

		}
	}
	self.mut.RUnlock()
	return Node[K, V]{}.Value, nil
}

func (self *GoStore[K, V]) Delete(key K) error {
	panic("Unimplemented")
}

// Replay replays the Write-Ahead Log and applies changes to the database.
func (self *GoStore[K, V]) Replay(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	for {
		var entry LogEntry[K, V]
		if err := dec.Decode(&entry); err != nil {
			break // End of log file
		}
		// Apply the entry to the database
		entry.Apply(self)
	}
	return nil
}

// Close closes all associated resources
func (self *GoStore[K, V]) Close() error {
	self.wal.Close()
	return nil
}
