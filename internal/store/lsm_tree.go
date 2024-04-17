package store

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

var WALPATH = filepath.Join("home", "$USER", "programming", "gostore", "WAL.db")

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
	// // ContainsKey returns true if the memTable contains the given key
	// ContainsKey(K) bool
	// Get returns a value associated with the key
	Get(K) (V, bool)
	// Returns the number of nodes in the memtable
	Size() uint
	// Clear points root to nil and makes size = 0
	Clear()
}

type LSMTree[K cmp.Ordered, V any] interface {
	// Write the Key-Value pair to the memtable
	Write(K, V)
	// Read the value from the given key
	Read(K)
	// Delete the key from the DB
	Delete(K)
	// Write the memtable to disk
	Flush()
}

func NewLSMTree[K cmp.Ordered, V any]() LSMTree[K, V] {
	wal, err := NewWal[K, V](WALPATH)
	if err != nil {
		panic(err)
	}
	return &GoStore[K, V]{memTable: &RedBlackTree[K, V]{}, wal: wal}
}

type GoStore[K cmp.Ordered, V any] struct {
	// MemTable is used by the LSMTree as an in-memory key-value store.
	//
	// The preferred implementation is a Red-Black tree but others may be
	// used.
	memTable MemTable[K, V]

	// Filenames of the currently unmerged segments
	segments []string

	// The max size before the memtable is flushed to disk
	max_size uint

	// The Write-Ahead-Log (wal) contains a log of all in-memory operations
	// prior to flushing. If the database crashes with data in-memory that has not
	// been written to disk, the current in-memory state may be recreated again after restart.
	wal *WAL[K, V]

	mut sync.RWMutex
}

// Write memTable to disk as SSTable
func (self *GoStore[K, V]) Flush() {
	// Persist in-memory data
	cacheDir, err := os.UserCacheDir()
	table := filepath.Join(cacheDir, "gostore", fmt.Sprintf("%v.segment", time.Now().Unix()))
	err = WriteSSTable(self.memTable, table)
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
func (self *GoStore[K, V]) Write(key K, val V) {
	self.mut.Lock()

	// Write to memTable
	self.memTable.Put(key, val)
	if self.exceeds_size() {
		go self.Flush()
		return
	}
	self.mut.Unlock()
}

func (self *GoStore[K, V]) Read(key K) {
	self.mut.RLock()
	// Read from memory
	if val, ok := self.memTable.Get(key); ok {
		// do something with val
		fmt.Printf("Not implemented, val: %v", val)
	} else {
		// Read from disk
		for _, filename := range self.segments {
			table, err := ReadSSTable[K, V](filename)
			if err != nil {
				break
			}
			if val, ok := table.Search(key); ok {
				fmt.Printf("val: %v", val)
				break
			} else {
				continue
			}

		}
	}
	self.mut.RUnlock()
}

func (self *GoStore[K, V]) Delete(key K) {
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
