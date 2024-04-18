package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
)

type BloomFilter[K cmp.Ordered] struct {
	bitset    []bool
	hashFuncs []hash.Hash64
}

// newBloomFilter creates a new Bloom filter with the specified size and number of hash functions.
func newBloomFilter[K cmp.Ordered](size int, numHashFuncs int) *BloomFilter[K] {
	return &BloomFilter[K]{
		bitset:    make([]bool, size),
		hashFuncs: createHashFuncs(numHashFuncs),
	}
}

// Add adds a key to the Bloom filter.
func (bf *BloomFilter[K]) Add(element K) {
	for _, hf := range bf.hashFuncs {
		hf.Write([]byte(fmt.Sprintf("%v", element)))
		index := hf.Sum64() % uint64(len(bf.bitset))
		bf.bitset[index] = true
	}
}

// Test tests whether a key is in the Bloom filter.
func (bf *BloomFilter[K]) Test(element K) bool {
	for _, hf := range bf.hashFuncs {
		hf.Write([]byte(fmt.Sprintf("%v", element)))
		index := hf.Sum64() % uint64(len(bf.bitset))
		if !bf.bitset[index] {
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

// Write saves the Bloom filter to a file.
func (bf *BloomFilter[K]) Write(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(bf); err != nil {
		return err
	}
	return nil
}

// loadBloomFromFile loads the Bloom filter from a file.
func loadBloomFromFile[K cmp.Ordered](filename string) (*BloomFilter[K], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bf := &BloomFilter[K]{}
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(bf); err != nil {
		return nil, err
	}
	return bf, nil
}
