package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
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
}

func (tbl *GostoreMemTable[K, V]) Iterator() Iterator[K, V] {
	return tbl.rbt.Iterator()
}

func (tbl *GostoreMemTable[K, V]) ExceedsSize() bool {
	return tbl.rbt.Size() >= tbl.max_size
}

func (tbl *GostoreMemTable[K, V]) Put(key K, val V) error {
	tbl.rbt.Put(key, val)
	return tbl.wal.Write(key, val)
}

func (tbl *GostoreMemTable[K, V]) Get(key K) (V, bool) {
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
	tbl.rbt.Delete(key)
}

func (tbl *GostoreMemTable[K, V]) Clone() MemTable[K, V] {
	newWalName := filepath.Join(filepath.Dir(tbl.wal.file.Name()), generateUniqueWALName())
	wal, err := newWal[K, V](newWalName)
	if err != nil {
		panic(err)
	}
	return &GostoreMemTable[K, V]{
		rbt:      &RedBlackTree[K, V]{},
		wal:      wal,
		max_size: tbl.max_size,
	}
}

// Restores database state from Write-Ahead-Log
func (self *GostoreMemTable[K, V]) Replay(filename string) error {
	self.rbt.Clear()
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	for {
		entry := []*LogEntry[K, V]{}
		if decodeErr := dec.Decode(&entry); decodeErr != nil {
			if decodeErr == io.EOF {
				break // End of log file
			}
			return &LogApplyErr[K, V]{Cause: decodeErr}
		}

		// Apply the entry to the database
		for _, e := range entry {
			e.Apply(self.rbt)
		}
	}
	return nil
}

// Returns an SSTable filled with entries, with no size
func (tbl *GostoreMemTable[K, V]) Snapshot(destDir string) *SSTable[K, V] {
	timestamp := time.Now()
	sstable := &SSTable[K, V]{
		Entries:   make([]*SSTableEntry[K, V], 0),
		Name:      filepath.Join(destDir, generateUniqueSegmentName(timestamp)),
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

type GoStoreMemTableOpts struct {
	walPath  string // Path to desired WAL location
	Max_size uint   // Max size before triggering flush
}

func NewGostoreMemTable[K cmp.Ordered, V any](opts *GoStoreMemTableOpts) (*GostoreMemTable[K, V], error) {
	wal, err := newWal[K, V](opts.walPath)
	if err != nil {
		return nil, err
	}
	memtable := &GostoreMemTable[K, V]{rbt: &RedBlackTree[K, V]{}, max_size: opts.Max_size, wal: wal}
	err = memtable.Replay(opts.walPath)
	return memtable, err
}
