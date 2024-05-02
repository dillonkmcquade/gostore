package lsm_tree

import (
	"testing"
)

func TestBloomFilterAdd(t *testing.T) {
	t.Run("Add int64", func(t *testing.T) {
		opts := &BloomFilterOpts{Size: 1000}
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
		opts := &BloomFilterOpts{Size: 100}
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
		opts := &BloomFilterOpts{Size: 100}
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

	t.Run("Add 15000", func(t *testing.T) {
		opts := &BloomFilterOpts{Size: 100000}
		filter := NewBloomFilter[int64](opts)

		for i := 0; i < 15000; i++ {
			filter.Add(int64(i))
		}

		for i := 0; i < 15000; i++ {
			if !filter.Has(int64(i)) {
				t.Errorf("Should contain %v", i)
			}
		}
	})

	t.Run("Add 30000", func(t *testing.T) {
		opts := &BloomFilterOpts{Size: 300000}
		filter := NewBloomFilter[int64](opts)

		for i := 0; i < 30000; i++ {
			filter.Add(int64(i))
		}

		for i := 0; i < 30000; i++ {
			if !filter.Has(int64(i)) {
				t.Errorf("Should contain %v", i)
			}
		}
	})
}

func TestBloomIO(t *testing.T) {
	t.Run("Save bloom to file", func(t *testing.T) {
		tmp := t.TempDir()
		opts := &BloomFilterOpts{Size: 1000, Path: tmp}
		filter := NewBloomFilter[int64](opts)

		keys := []int64{50, 70, 90}
		for _, key := range keys {
			filter.Add(key)
		}

		err := filter.Save()
		if err != nil {
			t.Errorf("error saving filter: %s", err)
		}

		filter.bitset = nil
		err = filter.Load()
		if err != nil {
			t.Error()
		}
		if !filter.Has(int64(50)) {
			t.Error("Should have key 50")
		}
	})
}
