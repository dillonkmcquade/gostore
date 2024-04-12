package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dillonkmcquade/gostore/internal/pb"
)

type RawRecord []byte // First byte represents the type, followed by the payload

type DataStore struct {
	mut  sync.RWMutex
	data map[string]RawRecord

	// Each key in the data store will also map to a channel used for destroying the goroutine responsible
	// for deleting it on expiration. These will be used when a record is deleted prior to expiration.
	cancelChans map[string]chan bool
}

func (self *DataStore) write(in *pb.WriteRequest, errChan chan<- error) {
	typeValue, ok := pb.Type_value[in.Type]

	if !ok {
		errChan <- errors.New(fmt.Sprintf("Invalid type %v", in.Type))
		return
	}

	b := byte(typeValue)

	in.Payload = append([]byte{b}, in.Payload...)

	self.mut.Lock()

	self.data[in.Key] = in.Payload

	// Create stop channel for destroying Clean routine later
	stopChan := make(chan bool, 1)
	self.cancelChans[in.Key] = stopChan

	self.mut.Unlock()

	expirationCtx, cancel := context.WithTimeout(context.Background(), time.Duration(in.Expiration)*time.Second)
	go self.Clean(expirationCtx, cancel, in.Key, stopChan)

	close(errChan)
}

func (self *DataStore) hasKey(key string) bool {
	_, hasKey := self.data[key]
	return hasKey
}

func (self *DataStore) read(key string, done chan<- any) {
	self.mut.RLock()
	v := self.data[key]
	self.mut.RUnlock()
	r, err := marshalRecord(v)
	if err != nil {
		done <- err
		return
	}
	done <- r
}

// Removes the record from the store and all associated resources (stopChan & Clean routine)
func (self *DataStore) delete(key string, done chan<- bool) {
	self.mut.Lock()

	delete(self.data, key)

	// Destroy goroutine
	self.cancelChans[key] <- true
	delete(self.cancelChans, key)

	self.mut.Unlock()
	done <- true
}

// Asynchronously waits to remove the record from the store after the specified time.
//
// Will return early if record is deleted prior to expiration
func (self *DataStore) Clean(ctx context.Context, cancel context.CancelFunc, key string, stop <-chan bool) {
	for {
		select {
		case <-ctx.Done():
			// TODO would be nice to find a way to not need a channel in this situation
			done := make(chan bool, 1)
			self.delete(key, done)
			<-done
			return
		case <-stop:
			cancel()
			return
		default:
			continue
		}
	}
}

// Decodes a byte array into a Record
func marshalRecord(r []byte) (*pb.Record, error) {
	t := int32(r[0])
	payload := r[1:]
	typ, ok := pb.Type_name[t]
	if !ok {
		return nil, errors.New("Error decoding type information from raw record")
	}
	return &pb.Record{Payload: payload, Type: typ}, nil
}
