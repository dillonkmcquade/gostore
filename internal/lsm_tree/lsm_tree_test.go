package lsm_tree

import "testing"

func TestNew(t *testing.T) {
	tree := New[int64, any](10)
	defer tree.Clean()
	defer tree.Close()
	err := tree.Write(1, "test")
	if err != nil {
		t.Error("Write should not return error")
	}
}
