package lsm_tree

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Implements MemTable[K,V]
type GostoreMemTable[K cmp.Ordered, V any] struct {
	rbt TreeMap[K, V]
	// The Write-Ahead-Log (wal) contains a log of all in-memory operations
	// prior to flushing. If the database crashes with data in-memory that has not
	// been written to disk, the current in-memory state may be recreated again after restart.
	wal        *WAL[K, V]
	max_size   uint
	bloom_size uint64
	bloomPath  string // Path to filters directory
}

func (mem *GostoreMemTable[K, V]) Iterator() Iterator[K, V] {
	return mem.rbt.Iterator()
}

func (mem *GostoreMemTable[K, V]) ExceedsSize() bool {
	return mem.rbt.Size() >= mem.max_size
}

func (mem *GostoreMemTable[K, V]) Put(key K, val V) error {
	mem.rbt.Put(key, val)
	return mem.wal.Write(key, val)
}

func (mem *GostoreMemTable[K, V]) Get(key K) (V, bool) {
	val, found := mem.rbt.Get(key)
	return val, found
}

func (mem *GostoreMemTable[K, V]) Size() uint {
	return mem.rbt.Size()
}

func (mem *GostoreMemTable[K, V]) Clear() {
	mem.rbt.Clear()
	mem.wal.Discard()
}

func (mem *GostoreMemTable[K, V]) Close() error {
	return mem.wal.Close()
}

func (mem *GostoreMemTable[K, V]) Delete(key K) {
	mem.rbt.Delete(key)
}

// Write memTable to disk as SSTable
func flush[K cmp.Ordered, V any](mem MemTable[K, V], destDir string) *SSTable[K, V] {
	// create sstable
	snapshot := mem.Snapshot(destDir)

	// save to file
	_, err := snapshot.Sync()
	if err != nil {
		slog.Error("flush: error syncing snapshot", "filename", snapshot.Name)
		panic(err)
	}
	err = snapshot.SaveFilter()
	if err != nil {
		slog.Error("flush: error saving filter", "filename", snapshot.Filter.Name)
		panic(err)
	}

	// Discard memTable & write-ahead log
	mem.Clear()
	return snapshot
}

// Restores database state from Write-Ahead-Log
func (mem *GostoreMemTable[K, V]) replay(filename string) error {
	path := filepath.Clean(filename)
	mem.rbt.Clear()
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("Replay: %v", err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	for {
		entry := make([]*LogEntry[K, V], mem.wal.batch_write_size)
		if decodeErr := dec.Decode(&entry); decodeErr != nil {
			if decodeErr == io.EOF {
				break // End of log file
			} else {
				return &LogApplyErr[K, V]{Cause: decodeErr}
			}
		}
		// Apply the entry to the database
		for _, e := range entry {
			e.Apply(mem.rbt)
			mem.wal.entryPool.Put(e)
		}

	}
	return nil
}

// Returns an SSTable filled with entries, with no size
func (mem *GostoreMemTable[K, V]) Snapshot(destDir string) *SSTable[K, V] {
	timestamp := time.Now()
	sstable := &SSTable[K, V]{
		Entries:   make([]*SSTableEntry[K, V], 0, mem.rbt.Size()),
		Name:      filepath.Join(destDir, generateUniqueSegmentName(timestamp)),
		Filter:    NewBloomFilter[K](&BloomFilterOpts{mem.bloom_size, mem.bloomPath}),
		CreatedOn: timestamp,
	}
	iter := mem.rbt.Iterator()
	for iter.HasNext() {
		node := iter.Next()
		entry := &SSTableEntry[K, V]{Key: node.Key, Value: node.Value, Operation: node.Operation}
		sstable.Entries = append(sstable.Entries, entry)
		sstable.Filter.Add(node.Key)
	}
	sstable.First = sstable.Entries[0].Key
	sstable.Last = sstable.Entries[len(sstable.Entries)-1].Key
	return sstable
}

type GoStoreMemTableOpts struct {
	Batch_write_size int    // number of entries to write at a time
	walPath          string // Path to desired WAL location
	Max_size         uint   // Max size before triggering flush
	Bloom_size       uint64
	BloomPath        string // path to filters directory
}

func NewGostoreMemTable[K cmp.Ordered, V any](opts *GoStoreMemTableOpts) (*GostoreMemTable[K, V], error) {
	wal, err := newWal[K, V](opts.walPath, opts.Batch_write_size)
	if err != nil {
		return nil, fmt.Errorf("NewGostoreMemTable: %v", err.Error())
	}
	memtable := &GostoreMemTable[K, V]{rbt: &RedBlackTree[K, V]{}, max_size: opts.Max_size, wal: wal, bloom_size: opts.Bloom_size, bloomPath: opts.BloomPath}
	err = memtable.replay(opts.walPath)
	if err != nil {
		return nil, err
	}
	return memtable, nil
}
