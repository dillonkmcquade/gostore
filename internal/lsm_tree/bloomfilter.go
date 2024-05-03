package lsm_tree

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
)

type BloomFilter[K cmp.Ordered] struct {
	bitset []uint64
	Name   string
	Size   uint64
}

type BloomFilterOpts struct {
	Size uint64
	Path string
}

func generateUniqueBloomName() string {
	string, err := generateRandomString(8)
	if err != nil {
		slog.Error("error while generating bloom filename")
		panic(err)
	}
	return fmt.Sprintf("bloom_%s.dat", string)
}

// NewBloomFilter creates a new Bloom filter with the specified size and number of hash functions.
func NewBloomFilter[K cmp.Ordered](opts *BloomFilterOpts) *BloomFilter[K] {
	gob.Register(fnv.New64a())
	assert(opts.Size > 0, "Bloom filter size cannot be 0")
	filter := &BloomFilter[K]{
		bitset: make([]uint64, (opts.Size+63)/64),
		Name:   filepath.Join(opts.Path, generateUniqueBloomName()),
		Size:   opts.Size,
	}
	return filter
}

func (bf *BloomFilter[K]) hashFunc(data []byte) uint64 {
	h := fnv.New64()
	n, err := h.Write(data)
	if err != nil || n != len(data) {
		slog.Error("hashFunc: error writing data to hash")
		panic(err)
	}
	return h.Sum64()
}

func ConvertToBytes(value interface{}) ([]byte, error) {
	if t, ok := value.(string); ok {
		return []byte(t), nil
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, value)
	return buf.Bytes(), err
}

// Add adds a key to the Bloom filter.
func (bf *BloomFilter[K]) Add(key K) {
	b, err := ConvertToBytes(key)
	if err != nil {
		slog.Error("Add: error converting key to byte array", "key", key)
		panic(err)
	}
	for _, hash := range bf.getHashes(b) {
		bf.bitset[hash/64] |= 1 << (hash % 64)
	}
}

func (bf *BloomFilter[K]) getHashes(data []byte) [2]uint64 {
	hash1 := bf.hashFunc(data)
	hash2 := hash1 >> 32 // Use the upper 32 bits of hash1
	return [2]uint64{hash1 % bf.Size, hash2 % bf.Size}
}

// Has tests whether a key is in the Bloom filter.
func (bf *BloomFilter[K]) Has(key K) bool {
	assert(bf.bitset != nil, "Bitset cannot be nil")

	b, err := ConvertToBytes(key)
	if err != nil {
		slog.Error("Has: error converting key to byte array", "key", key)
		panic(err)
	}
	for _, hash := range bf.getHashes(b) {
		if (bf.bitset[hash/64] & (1 << (hash % 64))) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter[K]) Load() error {
	if bf.bitset != nil {
		panic("bitset is not nil")
	}
	path := filepath.Clean(bf.Name)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	bitset := make([]uint64, (bf.Size+63)/64)
	defer file.Close()
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bitset)
	if err != nil {
		return err
	}
	bf.bitset = bitset
	return nil
}

// Save saves the Bloom filter to a file.
func (bf *BloomFilter[K]) Save() error {
	path := filepath.Clean(bf.Name)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("Save: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(bf.bitset); err != nil {
		return fmt.Errorf("Save: %v", err)
	}
	err = file.Sync()
	if err != nil {
		return fmt.Errorf("Save: %v", err)
	}
	return nil
}
