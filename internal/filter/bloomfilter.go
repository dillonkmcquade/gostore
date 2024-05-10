package filter

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dillonkmcquade/gostore/internal"
	"github.com/dillonkmcquade/gostore/internal/assert"
)

type BloomFilter struct {
	bitset []uint64
	Name   string
	Size   uint64
}

type Opts struct {
	Size uint64
	Path string
}

func GenerateUniqueBloomName() string {
	string, err := internal.GenerateRandomString(8)
	if err != nil {
		slog.Error("error while generating bloom filename")
		panic(err)
	}
	return fmt.Sprintf("bloom_%s.dat", string)
}

// New creates a new Bloom filter with the specified size and number of hash functions.
func New(opts *Opts) *BloomFilter {
	gob.Register(fnv.New64a())
	assert.True(opts.Size > 0, "Bloom filter size cannot be 0")
	filter := &BloomFilter{
		bitset: make([]uint64, (opts.Size+63)/64),
		Name:   filepath.Join(opts.Path, GenerateUniqueBloomName()),
		Size:   opts.Size,
	}
	return filter
}

func (bf *BloomFilter) hashFunc(data []byte) uint64 {
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
func (bf *BloomFilter) Add(key []byte) {
	for _, hash := range bf.getHashes(key) {
		bf.bitset[hash/64] |= 1 << (hash % 64)
	}
}

func (bf *BloomFilter) getHashes(data []byte) [2]uint64 {
	hash1 := bf.hashFunc(data)
	hash2 := hash1 >> 32 // Use the upper 32 bits of hash1
	return [2]uint64{hash1 % bf.Size, hash2 % bf.Size}
}

// Has tests whether a key is in the Bloom filter.
func (bf *BloomFilter) Has(key []byte) bool {
	assert.True(bf.bitset != nil, "Bitset cannot be nil")

	for _, hash := range bf.getHashes(key) {
		if (bf.bitset[hash/64] & (1 << (hash % 64))) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Load() error {
	path := filepath.Clean(bf.Name)
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open: %w", err)
	}
	bitset := make([]uint64, (bf.Size+63)/64)
	defer file.Close()
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bitset)
	if err != nil {
		return fmt.Errorf("decoder.Decode: %w", err)
	}
	bf.bitset = bitset
	return nil
}

// Save saves the Bloom filter to a file.
func (bf *BloomFilter) Save() error {
	path := filepath.Clean(bf.Name)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("os.Create: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(bf.bitset); err != nil {
		return fmt.Errorf("encoder.Encode: %w", err)
	}
	err = file.Sync()
	if err != nil {
		return fmt.Errorf("file.Sync: %w", err)
	}
	return nil
}

func (bf *BloomFilter) Clear() {
	bf.bitset = []uint64{}
}
