package lsm

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLSMNew(t *testing.T) {
	t.Run("Test opts", func(t *testing.T) {
		tmp := t.TempDir()
		tree, err := New[int64, any](NewTestLSMOpts(tmp))
		if err != nil {
			t.Error(err)
		}
		defer tree.Close()
	})

	t.Run("Default opts", func(t *testing.T) {
		tmp := t.TempDir()
		tree, err := New[int64, any](NewDefaultLSMOpts(tmp))
		if err != nil {
			t.Error(err)
		}
		defer tree.Close()
	})

	t.Run("Non-existing path", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("should panic")
			}
		}()
		_, err := New[int64, any](NewTestLSMOpts(""))
		if err != nil {
			t.Error(err)
		}
	})
}

func TestLSMWrite(t *testing.T) {
	t.Parallel()
	t.Run("1000", func(t *testing.T) {
		t.Parallel()
		tmp := t.TempDir()
		tree, err := New[int64, any](NewTestLSMOpts(tmp))
		if err != nil {
			t.Error(err)
		}
		defer tree.Close()
		var wg sync.WaitGroup
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				err := tree.Write(int64(i), "test")
				if err != nil {
					t.Error(err)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				_, err := tree.Read(int64(i))
				if err != nil {
					t.Errorf("Should be found: %v", i)
				}
			}(i)
		}
		wg.Done()
	})
	t.Run("1001", func(t *testing.T) {
		t.Parallel()
		tmp := t.TempDir()
		tree, err := New[int64, any](NewTestLSMOpts(tmp))
		if err != nil {
			t.Error(err)
		}
		defer tree.Close()
		var wg sync.WaitGroup
		for i := 0; i < 1001; i++ {
			wg.Add(1)
			go func(i int) {
				tree.Write(int64(i), "test")
				wg.Done()
			}(i)
		}
		wg.Wait()
		for i := 0; i < 1001; i++ {
			_, err := tree.Read(int64(i))
			if err != nil {
				t.Errorf("Should be found: %v", i)
			}
		}
	})
	t.Run("1999", func(t *testing.T) {
		t.Parallel()
		tmp := t.TempDir()
		tree, err := New[int64, any](NewTestLSMOpts(tmp))
		if err != nil {
			t.Error(err)
		}
		defer tree.Close()
		var wg sync.WaitGroup
		for i := 0; i < 1999; i++ {
			wg.Add(1)
			go func(i int) {
				tree.Write(int64(i), "test")
				wg.Done()
			}(i)
		}
		wg.Wait()
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
	tree, err := New[int64, any](NewTestLSMOpts(tmp))
	if err != nil {
		t.Error(err)
	}
	defer tree.Close()
	var wg sync.WaitGroup
	for i := 0; i < 11001; i++ {
		wg.Add(1)
		go func(i int) {
			tree.Write(int64(i), "test")
			wg.Done()
		}(i)
	}
	wg.Wait()
	_, err = tree.Read(11000)
	if err != nil {
		t.Error(err)
	}
}

func TestLSMFlush(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	opts := NewTestLSMOpts(tmp)
	opts.MemTableOpts.Max_size = 5
	tree, err := New[int64, any](opts)
	if err != nil {
		t.Error(err)
	}
	defer tree.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			tree.Write(int64(i), "test")
			wg.Done()
		}(i)
	}
	wg.Wait()
	tables, err := os.ReadDir(filepath.Join(tmp, "l0"))
	if err != nil {
		t.Error(err)
	}
	if len(tables) == 0 {
		t.Errorf("Segment directory should contain one SSTable, found %v", len(tables))
	}
}

func TestCompactedRead(t *testing.T) {
	tmp := t.TempDir()

	opts := NewTestLSMOpts(tmp)
	tree, err := New[int64, string](opts)
	if err != nil {
		t.Error(err)
	}

	defer tree.Close()

	for i := 0; i < 10000; i++ {
		err := tree.Write(int64(i), "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE")
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("Read from compacted tree - 1999", func(t *testing.T) {
		val, err := tree.Read(1999)
		if err != nil {
			t.Errorf("Reading %v: %v", 1999, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree - 0", func(t *testing.T) {
		val, err := tree.Read(0)
		if err != nil {
			t.Errorf("Reading %v: %v", 0, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree - 2000", func(t *testing.T) {
		val, err := tree.Read(2000)
		if err != nil {
			t.Errorf("Reading %v: %v", 2000, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree - 3000", func(t *testing.T) {
		val, err := tree.Read(3000)
		if err != nil {
			t.Errorf("Reading %v: %v", 3000, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree - 8000", func(t *testing.T) {
		val, err := tree.Read(8000)
		if err != nil {
			t.Errorf("Reading %v: %v", 8000, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree - 1111", func(t *testing.T) {
		val, err := tree.Read(1111)
		if err != nil {
			t.Errorf("Reading %v: %v", 1111, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
	t.Run("Read from compacted tree - 8888", func(t *testing.T) {
		val, err := tree.Read(8888)
		if err != nil {
			t.Errorf("Reading %v: %v", 8888, err)
			t.FailNow()
		}
		if val != "TESTTESTTESTTESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUETESTVALUE" {
			t.Error("Should be TESTVALUE")
		}
	})
}
