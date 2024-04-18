package lsm_tree

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	tree := New[int64, any](10)
	defer tree.Clean()
}

func TestLSMWrite(t *testing.T) {
	tree := New[int64, any](10)
	defer tree.Clean()
	for i := 0; i < 10; i++ {
		tree.Write(int64(i), "test")
	}
}

func TestLSMRead(t *testing.T) {
	tree := New[int64, any](10)
	defer tree.Clean()
	for i := 0; i < 10; i++ {
		tree.Write(int64(i), fmt.Sprintf("%vtest", i))
	}
	val, err := tree.Read(0)
	if err != nil || val != "0test" {
		t.Error(err)
	}
}

func TestLSMWriteWAL(t *testing.T) {
	tree := New[int64, any](100)
	defer tree.Clean()
	for i := 0; i < 10; i++ {
		err := tree.Write(int64(i), fmt.Sprintf("%vtest", i))
		if err != nil {
			t.Error(err)
		}
	}
}
