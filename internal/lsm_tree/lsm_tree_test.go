package lsm_tree

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tree := New[int64, any](100)
	defer tree.Clean()
}

func TestLSMWrite(t *testing.T) {
	tree := New[int64, any](100)
	defer tree.Clean()
	for i := 0; i < 9; i++ {
		tree.Write(int64(i), "test")
	}
}

func TestLSMRead(t *testing.T) {
	tree := New[int64, any](100)
	defer tree.Clean()
	for i := 0; i < 9; i++ {
		tree.Write(int64(i), fmt.Sprintf("%vtest", i))
	}
	val, err := tree.Read(0)
	if err != nil || val != "0test" {
		t.Error(err)
	}
}

func TestLSMFlush(t *testing.T) {
	tree := New[int64, any](5)
	defer tree.Close()

	for i := 0; i < 6; i++ {
		err := tree.Write(int64(i), "test")
		if err != nil {
			t.Error(err)
		}
	}
	time.Sleep(1 * time.Second)
	tables, err := os.ReadDir(level0)
	if err != nil {
		t.Error(err)
	}
	if len(tables) != 1 {
		t.Errorf("Segment directory should contain one SSTable, found %v", len(tables))
	}
}
