package lsm_tree

import (
	"path/filepath"
	"testing"
)

func TestBloomFilterAdd(t *testing.T) {
	t.Run("Add int64", func(t *testing.T) {
		opts := &BloomFilterOpts{size: 100, numHashFuncs: 3}
		bf := NewBloomFilter[int64](opts)

		integers := []int64{42, 123, 987}
		for _, num := range integers {
			bf.Add(num)
		}

		for _, num := range integers {
			if !bf.Has(num) {
				t.Errorf("Should contain %d", num)
			}
		}

		otherIntegers := []int64{1, 99, 1000}
		for _, num := range otherIntegers {
			if bf.Has(num) {
				t.Errorf("Shouldn't contain %d", num)
			}
		}
	})

	t.Run("Add int32", func(t *testing.T) {
		opts := &BloomFilterOpts{size: 100, numHashFuncs: 3}
		bf := NewBloomFilter[int32](opts)

		integers := []int32{42, 123, 987}
		for _, num := range integers {
			bf.Add(num)
		}

		for _, num := range integers {
			if !bf.Has(num) {
				t.Errorf("Should contain %d", num)
			}
		}

		otherIntegers := []int32{1, 99, 1000}
		for _, num := range otherIntegers {
			if bf.Has(num) {
				t.Errorf("Shouldn't contain %d", num)
			}
		}
	})

	t.Run("Add string", func(t *testing.T) {
		opts := &BloomFilterOpts{size: 100, numHashFuncs: 3}
		filter := NewBloomFilter[string](opts)

		strings := []string{"hello", "world", "bloom"}
		for _, str := range strings {
			filter.Add(str)
		}

		for _, str := range strings {
			if !filter.Has(str) {
				t.Errorf("Should contain %s", str)
			}
		}

		otherStrings := []string{"foo", "bar", "baz"}
		for _, str := range otherStrings {
			if filter.Has(str) {
				t.Errorf("Should not contain %s", str)
			}
		}
	})
}

func TestBloomFilterRemove(t *testing.T) {
	opts := &BloomFilterOpts{size: 100, numHashFuncs: 3}
	filter := NewBloomFilter[string](opts)

	strings := []string{"hello", "world", "bloom"}
	for _, str := range strings {
		filter.Add(str)
	}

	for _, string := range strings {
		filter.Remove(string)
		if filter.Has(string) {
			t.Errorf("Bloom filter should not contain %v", string)
		}

	}
}

func TestBloomIO(t *testing.T) {
	t.Run("Load bloom from file", func(t *testing.T) {
		filter, err := loadBloomFromFile[int]("../../data/bloom.dat")
		if err != nil {
			t.Errorf("Failed to load bloom from file: %s", err)
		}

		if !filter.Has(10) {
			t.Error("Should contain 10")
		}
	})

	t.Run("Save bloom to file", func(t *testing.T) {
		opts := &BloomFilterOpts{size: 100, numHashFuncs: 3}
		filter := NewBloomFilter[string](opts)

		strings := []string{"hello", "world", "bloom"}
		for _, str := range strings {
			filter.Add(str)
		}

		tmpDir := t.TempDir()
		filename := filepath.Join(tmpDir, "bloom.dat")
		err := filter.Save(filename)
		if err != nil {
			t.Errorf("error saving filter: %s", err)
		}

		loadedFilter, err := loadBloomFromFile[string](filename)
		if len(loadedFilter.HashFuncs) == 0 {
			t.Error("Should not be zero")
		}
		if err != nil {
			t.Errorf("Error loading filter from file")
		}
		if !loadedFilter.Has("hello") {
			t.Error("Should contain key hello")
		}
	})
}
