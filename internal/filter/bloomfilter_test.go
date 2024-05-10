package filter

import (
	"testing"
)

func TestBloomFilterAdd(t *testing.T) {
	t.Run("Add ", func(t *testing.T) {
		opts := &Opts{Size: 1000}
		bf := New(opts)

		integers := [][]byte{{100}, {1, 2, 3}, {9, 8, 7}}
		for _, num := range integers {
			bf.Add(num)
		}

		for _, num := range integers {
			if !bf.Has(num) {
				t.Errorf("Should contain %d", num)
			}
		}

		otherIntegers := [][]byte{{1}, {9, 9}, {1, 0, 0, 0}}
		for _, num := range otherIntegers {
			if bf.Has(num) {
				t.Errorf("Shouldn't contain %d", num)
			}
		}
	})
}

func TestBloomIO(t *testing.T) {
	t.Run("Save bloom to file", func(t *testing.T) {
		tmp := t.TempDir()
		opts := &Opts{Size: 1000, Path: tmp}
		filter := New(opts)

		keys := [][]byte{{50}, {70}, {90}}
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
		if !filter.Has([]byte{50}) {
			t.Error("Should have key 50")
		}
	})
}
