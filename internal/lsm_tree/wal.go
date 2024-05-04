package lsm_tree

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

type Operation byte

const (
	INSERT Operation = 0x49 // I
	DELETE Operation = 0x44 // D
)

type WAL[K cmp.Ordered, V any] struct {
	file             *os.File
	encoder          *json.Encoder
	writeChan        chan *LogEntry[K, V]
	batch_write_size int
	entryPool        *sync.Pool
	mut              sync.Mutex
	wg               sync.WaitGroup
}

// Generates a filename in the format WAL_UNIQUESTRING.dat
func generateUniqueWALName() string {
	uniqueString, err := generateRandomString(8)
	if err != nil {
		slog.Error("generateUniqueWALName: error generating random string")
		panic(err)
	}
	return fmt.Sprintf("WAL_%v.dat", uniqueString)
}

// Returns a new WAL. The WAL should be closed (with Close()) once it is no longer needed to remove allocated resources.
func newWal[K cmp.Ordered, V any](filename string, write_size int) (*WAL[K, V], error) {
	pool := &sync.Pool{
		New: func() any {
			return new(LogEntry[K, V])
		},
	}
	path := filepath.Clean(filename)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	wal := &WAL[K, V]{file: file, encoder: json.NewEncoder(file), writeChan: make(chan *LogEntry[K, V]), entryPool: pool, batch_write_size: write_size}
	wal.wg.Add(1)
	go wal.waitForWrites(write_size)
	return wal, nil
}

// Receives all entries over writeChan and writes a batch of log entries at a time to file
func (self *WAL[K, V]) waitForWrites(batchSize int) {
	batch := make([]*LogEntry[K, V], batchSize)
	count := 0
	defer func() {
		err := self.encoder.Encode(batch[:count])
		if err != nil {
			slog.Error("error encoding incomplete batch", "cause", err)
			return
		}
		err = self.file.Sync()
		if err != nil {
			slog.Error("error encoding syncing batch", "cause", err)
		}
		self.file.Close()
		self.wg.Done()
	}()
	for entry := range self.writeChan {
		batch[count] = entry
		count++
		if count >= batchSize {
			self.mut.Lock()
			err := self.encoder.Encode(batch)
			if err != nil {
				slog.Error("Error encoding WAL batch")
				panic(err)
			}
			err = self.file.Sync()
			if err != nil {
				slog.Error("Error syncing WAL file")
				panic(err)

			}
			for _, entry := range batch {
				self.entryPool.Put(entry)
			}
			self.mut.Unlock()
			count = 0
		}
	}
}

// Discards the contents of the current WAL
func (self *WAL[K, V]) Discard() error {
	self.mut.Lock()
	err := self.file.Truncate(0)
	if err != nil {
		slog.Error("error truncating file", "filename", self.file.Name())
		return err
	}
	_, err = self.file.Seek(0, 0)
	self.mut.Unlock()
	return err
}

func (self *WAL[K, V]) Size() (int64, error) {
	fd, err := self.file.Stat()
	if err != nil {
		return 0, err
	}
	return fd.Size(), nil
}

// Write writes a log entry to the Write-Ahead Log.
func (self *WAL[K, V]) Write(key K, val V) error {
	entry, ok := self.entryPool.Get().(*LogEntry[K, V])
	if ok {
		entry.Key = key
		entry.Value = val
		entry.Operation = INSERT // Deletes are not written to log because they can be removed from the memtable in memory
		self.writeChan <- entry
	} else {
		slog.Error("Retrieved invalid type from pool")
	}
	return nil
}

// Close closes the Write-Ahead Log file.
func (self *WAL[K, V]) Close() error {
	close(self.writeChan)
	self.wg.Wait()
	return nil
}

type LogEntry[K cmp.Ordered, V any] struct {
	Operation Operation
	Key       K
	Value     V
}

func (self *LogEntry[K, V]) Apply(rbt TreeMap[K, V]) {
	rbt.Put(self.Key, self.Value)
}

// LogApplyErr is returned when a log entry failed to be applied to be applied.
// This could indicate that some data was lost after a crash.
type LogApplyErr[K cmp.Ordered, V any] struct {
	Entry *LogEntry[K, V]
	Cause error
}

func (l *LogApplyErr[K, V]) Error() string {
	if l.Entry == nil {
		return fmt.Sprintf("Log apply error: %v", l.Cause)
	}
	return fmt.Sprintf("Error applying log entry operation '%v' with key %v and value %v: %v", l.Entry.Operation, l.Entry.Key, l.Entry.Value, l.Cause)
}
