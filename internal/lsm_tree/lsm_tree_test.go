package lsm_tree

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLSMNew(t *testing.T) {
	tmp := t.TempDir()
	tree := New[int64, any](NewTestLSMOpts(tmp))
	defer tree.Close()
}

func TestLSMWrite(t *testing.T) {
	t.Run("1000", func(t *testing.T) {
		tmp := t.TempDir()
		tree := New[int64, any](NewTestLSMOpts(tmp))
		defer tree.Close()
		for i := 0; i < 1000; i++ {
			tree.Write(int64(i), "test")
		}
		for i := 0; i < 1000; i++ {
			_, err := tree.Read(int64(i))
			if err != nil {
				t.Errorf("Should be found: %v", i)
			}
		}
	})
	t.Run("1001", func(t *testing.T) {
		tmp := t.TempDir()
		tree := New[int64, any](NewTestLSMOpts(tmp))
		defer tree.Close()
		for i := 0; i < 1001; i++ {
			tree.Write(int64(i), "test")
		}
		for i := 0; i < 1001; i++ {
			_, err := tree.Read(int64(i))
			if err != nil {
				t.Errorf("Should be found: %v", i)
			}
		}
	})
	t.Run("1999", func(t *testing.T) {
		tmp := t.TempDir()
		tree := New[int64, any](NewTestLSMOpts(tmp))
		defer tree.Close()
		for i := 0; i < 1999; i++ {
			tree.Write(int64(i), "test")
		}
		for i := 0; i < 1999; i++ {
			_, err := tree.Read(int64(i))
			if err != nil {
				t.Errorf("Should be found: %v", i)
			}
		}
	})
}

func TestLSMRead(t *testing.T) {
	tmp := t.TempDir()
	tree := New[int64, any](NewTestLSMOpts(tmp))
	defer tree.Close()
	for i := 0; i < 9; i++ {
		tree.Write(int64(i), fmt.Sprintf("%vtest", i))
	}
	val, err := tree.Read(0)
	if err != nil || val != "0test" {
		t.Error(err)
	}
}

func TestLSMFlush(t *testing.T) {
	tmp := t.TempDir()
	opts := NewTestLSMOpts(tmp)
	opts.MemTableOpts.Max_size = 5
	tree := New[int64, any](opts)
	defer tree.Close()

	for i := 0; i < 10; i++ {
		err := tree.Write(int64(i), "test")
		if err != nil {
			t.Error(err)
		}
	}
	time.Sleep(1 * time.Second)
	tables, err := os.ReadDir(filepath.Join(tmp, "l0"))
	if err != nil {
		t.Error(err)
	}
	if len(tables) == 0 {
		t.Errorf("Segment directory should contain one SSTable, found %v", len(tables))
	}
}
