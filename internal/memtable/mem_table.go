package memtable

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/ordered"
	"github.com/dillonkmcquade/gostore/internal/sstable"
)

// In-memory balanced key-value store
type MemTable[K cmp.Ordered, V any] interface {
	io.Closer
	Put(K, V) error                  // Insert Node to memTable
	Get(K) (V, bool)                 // Get returns a value associated with the key
	Delete(K)                        // Insert a node marked as delete
	Purge() []*sstable.SSTable[K, V] // Purge the memtable of any flushed tables
	Size() uint                      // Number of entries
	Clear()                          // Wipe the memtable
}

type GostoreMemTable[K cmp.Ordered, V any] struct {
	rbt           ordered.Collection[K, *sstable.Entry[K, V]] // Ordered in-memory data structure
	wal           *WAL[K, V]                                  // Log of all rbt operations
	flushedTables []*sstable.SSTable[K, V]                    // Flushed sstables that have not been added to L0 yet
	max_size      uint                                        // Max number of elements before flushing
	bloomOpts     *filter.Opts                                // Opts for creating a filter when a new table is created
	level0Dir     string                                      // Path to l0 directory
	writeChan     chan *sstable.Entry[K, V]                   // Process incoming write/delete requests
	mut           sync.RWMutex
	wg            sync.WaitGroup
}
type Opts struct {
	Batch_write_size int
	WalPath          string
	Max_size         uint
	FilterOpts       *filter.Opts
	LevelZero        string
}

func New[K cmp.Ordered, V any](opts *Opts) (MemTable[K, V], error) {
	wal, err := newWal[K, V](opts.WalPath, opts.Batch_write_size)
	if err != nil {
		return nil, fmt.Errorf("newWal: %w", err)
	}
	memtable := &GostoreMemTable[K, V]{
		rbt:       ordered.Rbt[K, *sstable.Entry[K, V]](),
		max_size:  opts.Max_size,
		wal:       wal,
		bloomOpts: opts.FilterOpts,
		level0Dir: opts.LevelZero,
		writeChan: make(chan *sstable.Entry[K, V]),
	}
	err = memtable.replay(opts.WalPath)
	if err != nil {
		return nil, err
	}
	go memtable.processWrites()
	return memtable, nil
}

func (mem *GostoreMemTable[K, V]) Purge() []*sstable.SSTable[K, V] {
	mem.mut.Lock()
	defer mem.mut.Unlock()
	tbls := mem.flushedTables
	mem.flushedTables = []*sstable.SSTable[K, V]{}
	return tbls
}

// Write memTable to disk as SSTable
func (mem *GostoreMemTable[K, V]) flush() {
	slog.Debug("Flushing")
	if !mem.shouldFlush() {
		slog.Warn("Attempt to flush memtable that should not flush")
		return
	}
	// create sstable
	snapshot := mem.Snapshot()

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

	mem.flushedTables = append(mem.flushedTables, snapshot)
	// Discard memTable & write-ahead log
	mem.Clear()
}

// Restores database state from Write-Ahead-Log
func (mem *GostoreMemTable[K, V]) replay(filename string) error {
	path := filepath.Clean(filename)
	mem.rbt.Clear()
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open: %w", err)
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
func (mem *GostoreMemTable[K, V]) Snapshot() *sstable.SSTable[K, V] {
	sstable := sstable.New(&sstable.Opts[K, V]{
		DestDir:   mem.level0Dir,
		BloomOpts: mem.bloomOpts,
	})

	iter := mem.rbt.Values()
	for iter.HasNext() {
		node := iter.Next()
		sstable.Entries = append(sstable.Entries, node)
		sstable.Filter.Add(node.Key)
	}
	sstable.First = sstable.Entries[0].Key
	sstable.Last = sstable.Entries[len(sstable.Entries)-1].Key
	return sstable
}

func (mem *GostoreMemTable[K, V]) processWrites() {
	for entry := range mem.writeChan {
		mem.mut.Lock()
		mem.rbt.Put(entry.Key, entry)
		err := mem.wal.Write(entry.Key, entry.Value)
		if err != nil {
			panic(fmt.Errorf("wal.Write: %w", err))
		}
		if mem.shouldFlush() {
			mem.flush()
		}
		mem.wg.Done()
		mem.mut.Unlock()
	}
}

func (mem *GostoreMemTable[K, V]) shouldFlush() bool {
	return mem.rbt.Size() >= mem.max_size
}

func (mem *GostoreMemTable[K, V]) Put(key K, val V) error {
	entry := &sstable.Entry[K, V]{Key: key, Value: val, Operation: sstable.INSERT}
	mem.wg.Add(1)
	mem.writeChan <- entry
	return nil
}

func (mem *GostoreMemTable[K, V]) Delete(key K) {
	placeholder := &sstable.Entry[K, V]{Key: key, Operation: sstable.DELETE}
	mem.wg.Add(1)
	mem.writeChan <- placeholder
}

func (mem *GostoreMemTable[K, V]) Get(key K) (V, bool) {
	mem.mut.RLock()
	defer mem.mut.RUnlock()
	if entry, found := mem.rbt.Get(key); found {
		if entry.Operation == sstable.DELETE {
			return sstable.Entry[K, V]{}.Value, false
		}
		return entry.Value, true
	}
	return sstable.Entry[K, V]{}.Value, false
}

func (mem *GostoreMemTable[K, V]) Size() uint {
	mem.mut.RLock()
	defer mem.mut.RUnlock()
	return mem.rbt.Size()
}

func (mem *GostoreMemTable[K, V]) Clear() {
	mem.rbt.Clear()
	mem.wal.Discard()
}

func (mem *GostoreMemTable[K, V]) Close() error {
	mem.wg.Wait()
	close(mem.writeChan)
	return mem.wal.Close()
}
