package store

import (
	"context"
	"errors"
	"sync"

	gostorepb "github.com/dillonkmcquade/gostore/proto"
)

type GoStore struct {
	gostorepb.UnimplementedGoStoreServer
	mut  sync.RWMutex
	data map[string]string
}

func New() *GoStore {
	return &GoStore{data: map[string]string{}}
}

func (self *GoStore) Write(ctx context.Context, in *gostorepb.WriteRequest) (*gostorepb.WriteReply, error) {
	if self.hasKey(in.Key) {
		return &gostorepb.WriteReply{Status: false, Message: "Existing key found"}, errors.New("Existing key found")
	}
	self.mut.Lock()
	self.write(in.Key, in.Payload)
	self.mut.Unlock()
	return &gostorepb.WriteReply{Status: true, Message: "Success"}, nil
}

func (self *GoStore) write(key string, value string) {
	self.data[key] = value
}

func (self *GoStore) hasKey(key string) bool {
	_, hasKey := self.data[key]
	return hasKey
}

func (self *GoStore) Read(ctx context.Context, in *gostorepb.ReadRequest) (*gostorepb.ReadReply, error) {
	if !self.hasKey(in.Key) {
		return nil, errors.New("Key not found")
	}
	v := self.read(in.Key)
	return &gostorepb.ReadReply{Value: v}, nil
}

func (self *GoStore) read(key string) string {
	self.mut.RLock()
	v := self.data[key]
	self.mut.RUnlock()
	return v
}

func (self *GoStore) Delete(ctx context.Context, in *gostorepb.ReadRequest) (*gostorepb.WriteReply, error) {
	return nil, nil
}

func (self *GoStore) Update(ctx context.Context, in *gostorepb.WriteRequest) (*gostorepb.WriteReply, error) {
	return nil, nil
}
