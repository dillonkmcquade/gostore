package lsm_tree

import (
	"path/filepath"
	"testing"
)

func TestBloomFilterAdd(t *testing.T) {
	t.Run("Add int", func(t *testing.T) {
		bf := NewBloomFilter[int](100, 3)

		integers := []int{42, 123, 987}
		for _, num := range integers {
			bf.Add(num)
		}

		for _, num := range integers {
			if !bf.Has(num) {
				t.Errorf("Should contain %d", num)
			}
		}

		otherIntegers := []int{1, 99, 1000}
		for _, num := range otherIntegers {
			if bf.Has(num) {
				t.Errorf("Shouldn't contain %d", num)
			}
		}
	})

	t.Run("Add string", func(t *testing.T) {
		filter := NewBloomFilter[string](100, 3)

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
		tmpDir := t.TempDir()
		filter := NewBloomFilter[string](200, 3)

		strings := []string{"hello", "world", "bloom"}
		for _, str := range strings {
			filter.Add(str)
		}

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
