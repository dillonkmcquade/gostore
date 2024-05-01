package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"sync"
)

type Operation byte

const (
	INSERT Operation = 0x49 // I
	DELETE Operation = 0x44 // D
)

type WAL[K cmp.Ordered, V any] struct {
	file             *os.File
	encoder          *gob.Encoder
	writeChan        chan *LogEntry[K, V]
	batch_write_size int
	mut              sync.Mutex
}

func generateUniqueWALName() string {
	uniqueString, err := generateRandomString(8)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("WAL_%v.dat", uniqueString)
}

// Returns a new WAL. The WAL should be closed (with Close()) once it is no longer needed to remove allocated resources.
func newWal[K cmp.Ordered, V any](filename string, write_size int) (*WAL[K, V], error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	wal := &WAL[K, V]{file: file, encoder: gob.NewEncoder(file), writeChan: make(chan *LogEntry[K, V])}
	go wal.waitForWrites(write_size)
	return wal, err
}

func (self *WAL[K, V]) waitForWrites(batchSize int) {
	batch := make([]*LogEntry[K, V], 0)
	for entry := range self.writeChan {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			self.mut.Lock()
			err := self.encoder.Encode(batch)
			if err != nil {
				slog.Error("error", "cause", err)
			}
			batch = []*LogEntry[K, V]{}
			err = self.file.Sync()
			if err != nil {
				slog.Error("error", "cause", err)
			}
			slog.Info("Sync", "filename", self.file.Name())
			self.mut.Unlock()
		}
	}
	defer close(self.writeChan)
}

// Discards the contents of the current WAL
func (self *WAL[K, V]) Discard() error {
	err := self.file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = self.file.Seek(0, 0)
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
	entry := &LogEntry[K, V]{Key: key, Value: val, Operation: INSERT}
	self.writeChan <- entry
	return nil
}

// Close closes the Write-Ahead Log file.
func (self *WAL[K, V]) Close() error {
	return self.file.Close()
}

type LogEntry[K cmp.Ordered, V any] struct {
	Operation Operation
	Key       K
	Value     V
}

func (self *LogEntry[K, V]) Apply(rbt *RedBlackTree[K, V]) {
	switch self.Operation {
	case INSERT:
		rbt.Put(self.Key, self.Value)
	case DELETE:
		panic("Unimplemented")
	}
}

// LogApplyErr is returned when a log entry failed to be applied to be applied.
// This could indicate that some data was lost after a crash.
type LogApplyErr[K cmp.Ordered, V any] struct {
	Entry *LogEntry[K, V]
	Cause error
}

func (l *LogApplyErr[K, V]) Error() string {
	return fmt.Sprintf("Error applying log entry operation '%v' with key %v and value %v: %v", l.Entry.Operation, l.Entry.Key, l.Entry.Value, l.Cause)
}
