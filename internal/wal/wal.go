package wal

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/dillonkmcquade/gostore/internal"
	"google.golang.org/protobuf/proto"
)

// The write ahead log is responsible for logging all memtable operations.
// In the event of a crash, the log file will be used to recreate the previous memtable state.
// Entries are json encoded in batches in a separate goroutine.
type WAL[T LogEntry] struct {
	file             *os.File
	encoder          *json.Encoder
	writeChan        chan T
	Batch_write_size int
	mut              sync.Mutex
	wg               sync.WaitGroup
	done             chan bool
}

type LogEntry interface {
	Apply(interface{}) error
	MarshalProto() proto.Message
}

// Generates a filename in the format WAL_UNIQUESTRING.dat
func generateUniqueWALName() string {
	uniqueString, err := internal.GenerateRandomString(8)
	if err != nil {
		slog.Error("generateUniqueWALName: error generating random string")
		panic(err)
	}
	return fmt.Sprintf("WAL_%v.dat", uniqueString)
}

// Returns a new WAL. The WAL should be closed (with Close()) once it is no longer needed to remove allocated resources.
func New[T LogEntry](filename string, write_size int) (*WAL[T], error) {
	path := filepath.Clean(filename)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	wal := &WAL[T]{file: file, encoder: json.NewEncoder(file), writeChan: make(chan T), Batch_write_size: write_size, done: make(chan bool, 1)}
	wal.wg.Add(1)
	go wal.waitForWrites(write_size)
	return wal, nil
}

// Receives all entries over writeChan and writes a batch of log entries at a time to file
func (self *WAL[T]) waitForWrites(batchSize int) {
	batch := make([]T, batchSize)
	count := 0

	for {
		select {
		// Finish batch if incomplete on program exit
		case <-self.done:
			defer slog.Info("Batch write thread finished")
			if count == 0 {
				slog.Info("return early")
				self.wg.Done()
				return
			}
			self.mut.Lock()
			defer self.mut.Unlock()
			writer := NewBatchWriter(self.file)
			for _, e := range batch[:count] {
				writer.Write(e)
			}
			if err := writer.Err(); err != nil {
				slog.Error("batch write error", "cause", err)
			}
			err := self.file.Sync()
			if err != nil {
				slog.Error("error encoding syncing batch", "cause", err)
			}
			if err := self.file.Close(); err != nil {
				slog.Error(err.Error())
			}
			self.wg.Done()
			return
		// Batch queue
		case entry := <-self.writeChan:
			batch[count] = entry
			count++
			if count >= batchSize {
				self.mut.Lock()
				writer := NewBatchWriter(self.file)
				for _, e := range batch {
					writer.Write(e)
				}
				if err := writer.Err(); err != nil {
					slog.Error("writer error", "cause", err)
					panic(err)
				}
				err := self.file.Sync()
				if err != nil {
					slog.Error("Error syncing WAL file")
					panic(err)

				}
				self.mut.Unlock()
				count = 0

			}
		}
	}
}

// Discards the contents of the current WAL
func (self *WAL[T]) Discard() error {
	self.mut.Lock()
	defer self.mut.Unlock()
	err := self.file.Truncate(0)
	if err != nil {
		slog.Error("error truncating file", "filename", self.file.Name())
		return fmt.Errorf("file.Truncate: %w", err)
	}
	_, err = self.file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("file.Seek: %w", err)
	}
	return nil
}

// Returns the size in bytes of the Write-Ahead Log
func (self *WAL[T]) Size() (int64, error) {
	fd, err := self.file.Stat()
	if err != nil {
		return 0, err
	}
	return fd.Size(), nil
}

// Write writes a log entry to the Write-Ahead Log.
func (self *WAL[T]) Write(entry T) error {
	self.writeChan <- entry
	return nil
}

// Close closes the writeChan, and waits for the queued writes to finish.
func (self *WAL[T]) Close() error {
	close(self.writeChan)
	self.done <- true
	self.wg.Wait()
	return nil
}

// LogApplyErr is returned when a log entry failed to be applied to be applied.
// This could indicate that some data was lost after a crash.
type LogApplyErr struct {
	Cause error
}

func (l *LogApplyErr) Error() string {
	return fmt.Sprintf("Log apply error: %v", l.Cause)
}

func (l *LogApplyErr) Unwrap() error {
	return l.Cause
}
