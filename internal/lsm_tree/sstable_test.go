package lsm_tree

import "testing"

func TestSSTableOverlaps(t *testing.T) {
	t.Run("Should not overlap", func(t *testing.T) {
		t.Parallel()
		t1 := &SSTable[int64, string]{
			First: 0,
			Last:  9,
		}
		t2 := &SSTable[int64, string]{
			First: 10,
			Last:  19,
		}
		if t1.Overlaps(t2) {
			t.Errorf("Should not overlap")
		}
	})
	t.Run("T1 should overlap T2", func(t *testing.T) {
		t.Parallel()
		t1 := &SSTable[int64, string]{
			First: 0,
			Last:  11,
		}
		t2 := &SSTable[int64, string]{
			First: 10,
			Last:  19,
		}
		if !t1.Overlaps(t2) {
			t.Errorf("T1 should overlap T2")
		}
		if !t2.Overlaps(t1) {
			t.Error("T2 should overlap t1")
		}
	})
}
