package lsm_tree

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAddInt(t *testing.T) {
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
}

func TestAddString(t *testing.T) {
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
}

func TestLoadBloomFromFile(t *testing.T) {
	home, _ := os.UserHomeDir()
	filename := filepath.Join(home, "programming", "gostore", "data", "bloom.dat")
	filter, err := loadBloomFromFile[int](filename)
	if err != nil {
		t.Errorf("Failed to load bloom from file: %s", err)
	}

	if !filter.Has(10) {
		t.Error("Should contain 10")
	}
}

func TestSave(t *testing.T) {
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
}
