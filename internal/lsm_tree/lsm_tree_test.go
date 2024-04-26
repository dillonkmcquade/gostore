package lsm_tree

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestLSMNew(t *testing.T) {
	opts := &NewLSMOpts{
		BloomOpts: &BloomFilterOpts{
			size:         100,
			numHashFuncs: 3,
			path:         "",
		},
		ManifestOpts: &ManifestOpts{
			path:            manifestPath,
			num_levels:      4,
			level0_max_size: LEVEL0_MAX_SIZE,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  walPath,
			max_size: 100,
		},
	}
	tree := New[int64, any](opts)
	defer tree.Clean()
}

func TestLSMWrite(t *testing.T) {
	opts := &NewLSMOpts{
		BloomOpts: &BloomFilterOpts{
			size:         100,
			numHashFuncs: 3,
			path:         "",
		},
		ManifestOpts: &ManifestOpts{
			path:            manifestPath,
			num_levels:      4,
			level0_max_size: LEVEL0_MAX_SIZE,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  walPath,
			max_size: 100,
		},
	}
	tree := New[int64, any](opts)
	defer tree.Clean()
	for i := 0; i < 9; i++ {
		tree.Write(int64(i), "test")
	}
}

func TestLSMRead(t *testing.T) {
	opts := &NewLSMOpts{
		BloomOpts: &BloomFilterOpts{
			size:         100,
			numHashFuncs: 3,
			path:         "",
		},
		ManifestOpts: &ManifestOpts{
			path:            manifestPath,
			num_levels:      4,
			level0_max_size: LEVEL0_MAX_SIZE,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  walPath,
			max_size: 100,
		},
	}
	tree := New[int64, any](opts)
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
	opts := &NewLSMOpts{
		BloomOpts: &BloomFilterOpts{
			size:         100,
			numHashFuncs: 3,
			path:         "",
		},
		ManifestOpts: &ManifestOpts{
			path:            manifestPath,
			num_levels:      4,
			level0_max_size: LEVEL0_MAX_SIZE,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  walPath,
			max_size: 5,
		},
	}
	tree := New[int64, any](opts)
	defer tree.Clean()

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
