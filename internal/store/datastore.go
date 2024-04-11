package store

import "sync"

type DataStore struct {
	mut  sync.RWMutex
	data map[string]string
}

func (self *DataStore) write(key string, value string) {
	self.data[key] = value
}

func (self *DataStore) hasKey(key string) bool {
	_, hasKey := self.data[key]
	return hasKey
}

func (self *DataStore) read(key string) string {
	self.mut.RLock()
	v := self.data[key]
	self.mut.RUnlock()
	return v
}

func (self *DataStore) delete(key string) {
	self.mut.Lock()
	delete(self.data, key)
	self.mut.Unlock()
}
