package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
)

type BloomFilter[K cmp.Ordered] struct {
	Bitset    []bool
	HashFuncs []hash.Hash64
}

// NewBloomFilter creates a new Bloom filter with the specified size and number of hash functions.
func NewBloomFilter[K cmp.Ordered](size int, numHashFuncs int) *BloomFilter[K] {
	gob.Register(fnv.New64a())
	return &BloomFilter[K]{
		Bitset:    make([]bool, size),
		HashFuncs: createHashFuncs(numHashFuncs),
	}
}

// Add adds a key to the Bloom filter.
func (bf *BloomFilter[K]) Add(key K) {
	for _, hf := range bf.HashFuncs {
		hf.Reset()
		hf.Write([]byte(fmt.Sprintf("%v", key)))
		index := hf.Sum64() % uint64(len(bf.Bitset))
		bf.Bitset[index] = true
	}
}

// Has tests whether a key is in the Bloom filter.
func (bf *BloomFilter[K]) Has(key K) bool {
	for _, hf := range bf.HashFuncs {
		hf.Reset()
		hf.Write([]byte(fmt.Sprintf("%v", key)))
		index := hf.Sum64() % uint64(len(bf.Bitset))
		if !bf.Bitset[index] {
			return false
		}
	}
	return true
}

// createHashFuncs creates a set of hash functions based on FNV-1a.
func createHashFuncs(numHashFuncs int) []hash.Hash64 {
	hashFuncs := make([]hash.Hash64, numHashFuncs)
	for i := 0; i < numHashFuncs; i++ {
		hashFuncs[i] = fnv.New64a()
	}
	return hashFuncs
}

// Save saves the Bloom filter to a file.
func (bf *BloomFilter[K]) Save(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(bf); err != nil {
		return err
	}
	err = file.Sync()
	return err
}

// loadBloomFromFile loads the Bloom filter from a file.
func loadBloomFromFile[K cmp.Ordered](filename string) (*BloomFilter[K], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var bf BloomFilter[K]
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&bf); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}
	}
	return &bf, nil
}
