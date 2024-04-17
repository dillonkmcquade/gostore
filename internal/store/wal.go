package store

import (
	"cmp"
	"encoding/gob"
	"os"
	"sync"
)

type LogEntry[K cmp.Ordered, V any] struct {
	Operation string
	Key       K
	Value     V
}

func (self *LogEntry[K, V]) Apply(lsm LSMTree[K, V]) error {
	switch self.Operation {
	case "insert":
		lsm.Write(self.Key, self.Value)
	case "delete":
		lsm.Delete(self.Key)
	}
	return nil
}

type WAL[K cmp.Ordered, V any] struct {
	file    *os.File
	encoder *gob.Encoder
	mut     sync.Mutex
}

// Returns a new WAL. The WAL should be closed (with Close()) once it is no longer needed to remove allocated resources.
func NewWal[K cmp.Ordered, V any](filename string) (*WAL[K, V], error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	return &WAL[K, V]{file: file, encoder: gob.NewEncoder(file)}, err
}

// Discards the contents of the current WAL
func (self *WAL[K, V]) Discard() error {
	self.mut.Lock()
	err := self.file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = self.file.Seek(0, 0)
	self.mut.Unlock()
	return err
}

// Write writes a log entry to the Write-Ahead Log.
func (self *WAL[K, V]) Write(key K, val V) error {
	entry := &LogEntry[K, V]{Key: key, Value: val, Operation: "insert"}
	self.mut.Lock()
	err := self.encoder.Encode(entry)
	if err != nil {
		return err
	}
	// Ensure the entry is flushed to disk immediately.
	err = self.file.Sync()
	self.mut.Unlock()
	return err
}

// Close closes the Write-Ahead Log file.
func (self *WAL[K, V]) Close() error {
	return self.file.Close()
}
