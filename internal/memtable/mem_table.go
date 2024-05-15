package memtable

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/ordered"
	"github.com/dillonkmcquade/gostore/internal/pb"
	"github.com/dillonkmcquade/gostore/internal/sstable"
	"github.com/dillonkmcquade/gostore/internal/wal"
	"google.golang.org/protobuf/proto"
)

// In-memory balanced key-value store
type MemTable interface {
	io.Closer
	Put([]byte, []byte) error  // Insert Node to memTable
	Get([]byte) ([]byte, bool) // Get returns a value associated with the key
	Delete([]byte)             // Insert a node marked as delete
	Size() uint                // Number of entries
	Clear()                    // Wipe the memtable

	FlushedTables() <-chan *sstable.SSTable
}

type GostoreMemTable struct {
	rbt       ordered.Collection[[]byte, *pb.SSTable_Entry] // Ordered in-memory data structure
	wal       *wal.WAL[*pb.SSTable_Entry]                   // Log of all rbt operations
	max_size  uint                                          // Max number of elements before flushing
	bloomOpts *filter.Opts                                  // Opts for creating a filter when a new table is created
	level0Dir string                                        // Path to l0 directory
	flushChan chan *sstable.SSTable                         // Flushed sstables that have not been added to L0 yet
	writeChan chan *pb.SSTable_Entry                        // Process incoming write/delete requests
	mut       sync.RWMutex
	wg        sync.WaitGroup
}
type Opts struct {
	Batch_write_size int
	WalPath          string
	Max_size         uint
	FilterOpts       *filter.Opts
	LevelZero        string
}

func New(opts *Opts) (MemTable, error) {
	wal, err := wal.New[*pb.SSTable_Entry](opts.WalPath, opts.Batch_write_size)
	if err != nil {
		return nil, fmt.Errorf("newWal: %w", err)
	}
	memtable := &GostoreMemTable{
		rbt:       ordered.Rbt[[]byte, *pb.SSTable_Entry](slices.Compare[[]byte]),
		max_size:  opts.Max_size,
		wal:       wal,
		bloomOpts: opts.FilterOpts,
		level0Dir: opts.LevelZero,
		writeChan: make(chan *pb.SSTable_Entry),
		flushChan: make(chan *sstable.SSTable),
	}
	err = memtable.replay(opts.WalPath)
	if err != nil {
		return nil, err
	}
	go memtable.processWrites()
	return memtable, nil
}

// Write memTable to disk as SSTable
func (mem *GostoreMemTable) flush() {
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

	slog.Debug("Sending snapshot over flushChan")
	mem.flushChan <- snapshot
	mem.wg.Done()

	// Discard memTable & write-ahead log
	mem.Clear()
}

// Restores database state from Write-Ahead-Log
func (mem *GostoreMemTable) replay(filename string) error {
	path := filepath.Clean(filename)
	mem.rbt.Clear()
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(wal.SplitProtobuf)
	for scanner.Scan() {
		var e pb.SSTable_Entry
		err := proto.Unmarshal(scanner.Bytes(), &e)
		if err != nil {
			return fmt.Errorf("proto.Unmarshal: %w", err)
		}
		err = e.Apply(mem.rbt)
		if err != nil {
			slog.Error("log apply error", "cause", err)
			return &wal.LogApplyErr{Cause: err}
		}

	}
	if err := scanner.Err(); err != nil {
		slog.Error("scanner error", "cause", err)
		return err
	}
	return nil
}

func (mem *GostoreMemTable) FlushedTables() <-chan *sstable.SSTable {
	return mem.flushChan
}

// Returns an SSTable filled with entries, with no size
func (mem *GostoreMemTable) Snapshot() *sstable.SSTable {
	sstable := sstable.New(&sstable.Opts{
		DestDir:   mem.level0Dir,
		BloomOpts: mem.bloomOpts,
		Entries:   make([]*pb.SSTable_Entry, 0, mem.rbt.Size()),
	})

	for node := range mem.rbt.Values() {
		sstable.Entries = append(sstable.Entries, node)
		sstable.Filter.Add(node.Key)
	}
	sstable.First = sstable.Entries[0].Key
	sstable.Last = sstable.Entries[len(sstable.Entries)-1].Key
	return sstable
}

func (mem *GostoreMemTable) processWrites() {
	for entry := range mem.writeChan {
		mem.mut.Lock()
		mem.rbt.Put(entry.Key, entry)
		err := mem.wal.Write(entry)
		if err != nil {
			panic(fmt.Errorf("wal.Write: %w", err))
		}
		if mem.shouldFlush() {
			mem.wg.Add(1)
			mem.flush()
		}
		mem.wg.Done()
		mem.mut.Unlock()
	}
}

func (mem *GostoreMemTable) shouldFlush() bool {
	return mem.rbt.Size() >= mem.max_size
}

func (mem *GostoreMemTable) Put(key []byte, val []byte) error {
	entry := &pb.SSTable_Entry{Key: key, Value: val, Op: pb.Operation_OPERATION_INSERT}
	mem.wg.Add(1)
	mem.writeChan <- entry
	return nil
}

func (mem *GostoreMemTable) Delete(key []byte) {
	placeholder := &pb.SSTable_Entry{Key: key, Value: []byte{}, Op: pb.Operation_OPERATION_DELETE}
	mem.wg.Add(1)
	mem.writeChan <- placeholder
}

func (mem *GostoreMemTable) Get(key []byte) ([]byte, bool) {
	mem.mut.RLock()
	defer mem.mut.RUnlock()
	if entry, found := mem.rbt.Get(key); found {
		if entry.Op == pb.Operation_OPERATION_DELETE {
			return []byte{}, false
		}
		return entry.Value, true
	}
	return []byte{}, false
}

func (mem *GostoreMemTable) Size() uint {
	mem.mut.RLock()
	defer mem.mut.RUnlock()
	return mem.rbt.Size()
}

func (mem *GostoreMemTable) Clear() {
	mem.rbt.Clear()
	err := mem.wal.Discard()
	if err != nil {
		panic(err)
	}
}

func (mem *GostoreMemTable) Close() error {
	mem.wg.Wait()
	close(mem.writeChan)
	close(mem.flushChan)
	if err := mem.wal.Close(); err != nil {
		return err
	}
	return nil
}
