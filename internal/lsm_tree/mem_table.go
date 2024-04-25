package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Implements MemTable[K,V]
type GostoreMemTable[K cmp.Ordered, V any] struct {
	rbt *RedBlackTree[K, V]
	// The Write-Ahead-Log (wal) contains a log of all in-memory operations
	// prior to flushing. If the database crashes with data in-memory that has not
	// been written to disk, the current in-memory state may be recreated again after restart.
	wal      *WAL[K, V]
	max_size uint
	mut      sync.RWMutex
}

func (tbl *GostoreMemTable[K, V]) Iterator() Iterator[K, V] {
	return tbl.rbt.Iterator()
}

func (tbl *GostoreMemTable[K, V]) ExceedsSize() bool {
	return tbl.rbt.Size() > tbl.max_size
}

func (tbl *GostoreMemTable[K, V]) Put(key K, val V) error {
	tbl.mut.Lock()
	defer tbl.mut.Unlock()
	tbl.rbt.Put(key, val)
	return tbl.wal.Write(key, val)
}

func (tbl *GostoreMemTable[K, V]) Get(key K) (V, bool) {
	tbl.mut.RLock()
	defer tbl.mut.RUnlock()
	val, found := tbl.rbt.Get(key)
	return val, found
}

func (tbl *GostoreMemTable[K, V]) Size() uint {
	return tbl.rbt.Size()
}

func (tbl *GostoreMemTable[K, V]) Clear() {
	tbl.rbt.Clear()
	tbl.wal.Discard()
}

func (tbl *GostoreMemTable[K, V]) Close() error {
	return tbl.wal.Close()
}

func (tbl *GostoreMemTable[K, V]) Delete(key K) {
	tbl.mut.Lock()
	defer tbl.mut.Unlock()
	tbl.rbt.Delete(key)
}

// Replay replays the Write-Ahead Log and applies changes to the database.
func (self *GostoreMemTable[K, V]) Replay(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	for {
		entry := &LogEntry[K, V]{}
		if decodeErr := dec.Decode(entry); decodeErr != nil {
			if decodeErr == io.EOF {
				break // End of log file
			}
			return &LogApplyErr[K, V]{Entry: entry, Cause: decodeErr}
		}

		// Apply the entry to the database
		entry.Apply(self.rbt)
	}
	return nil
}

// Returns an SSTable filled with entries, with no size
func (tbl *GostoreMemTable[K, V]) Snapshot() *SSTable[K, V] {
	tbl.mut.Lock()
	defer tbl.mut.Unlock()
	timestamp := time.Now()
	sstable := &SSTable[K, V]{
		Entries:   make([]*SSTableEntry[K, V], 0),
		Name:      filepath.Join(level0, fmt.Sprintf("%v.segment", timestamp.Unix())),
		CreatedOn: timestamp,
	}
	iter := tbl.rbt.Iterator()
	for iter.HasNext() {
		node := iter.Next()
		entry := &SSTableEntry[K, V]{Key: node.Key, Value: node.Value, Operation: node.Operation}
		sstable.Entries = append(sstable.Entries, entry)
	}
	sstable.First = sstable.Entries[0].Key
	sstable.Last = sstable.Entries[len(sstable.Entries)-1].Key
	return sstable
}

func NewGostoreMemTable[K cmp.Ordered, V any](max_size uint) (*GostoreMemTable[K, V], error) {
	wal, err := newWal[K, V](walPath)
	if err != nil {
		return nil, err
	}
	memtable := &GostoreMemTable[K, V]{rbt: &RedBlackTree[K, V]{}, max_size: max_size, wal: wal}
	err = memtable.Replay(walPath)
	return memtable, err
}
