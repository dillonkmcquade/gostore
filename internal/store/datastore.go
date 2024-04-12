package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/pb"
)

type (
	RawRecord []byte // First byte represents the type, followed by the payload
	Record    struct {
		RecordType string // The string representation of pb.Type, indicates the type of the payload
		Payload    []byte
	}
)

type DataStore struct {
	mut  sync.RWMutex
	data map[string]RawRecord
}

func (self *DataStore) write(key string, value RawRecord, t string) error {
	typeValue, ok := pb.Type_value[t]
	if !ok {
		return errors.New(fmt.Sprintf("Invalid type %v", t))
	}
	b := byte(typeValue)
	value = append([]byte{b}, value...)

	self.mut.Lock()
	self.data[key] = value
	self.mut.Unlock()
	return nil
}

func (self *DataStore) hasKey(key string) bool {
	_, hasKey := self.data[key]
	return hasKey
}

func (self *DataStore) read(key string) (*Record, error) {
	self.mut.RLock()
	v := self.data[key]
	self.mut.RUnlock()
	return decodeRawRecord(v)
}

// Decodes a byte array into a Record
func decodeRawRecord(r []byte) (*Record, error) {
	t := int32(r[0])
	payload := r[1:]
	typ, ok := pb.Type_name[t]
	if !ok {
		return nil, errors.New("Error decoding type information from raw record")
	}
	return &Record{Payload: payload, RecordType: typ}, nil
}

func (self *DataStore) delete(key string) {
	self.mut.Lock()
	delete(self.data, key)
	self.mut.Unlock()
}
