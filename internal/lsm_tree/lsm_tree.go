package lsm_tree

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type GoStore[K cmp.Ordered, V any] struct {
	// The current memtable
	memTable MemTable[K, V]

	// Filenames of sstables ordered oldest to most recent
	manifest *Manifest[K, V]

	// Configurable interface for compaction and compaction trigger
	compaction CompactionController[K, V]

	// Verify if the key exists in the DB quickly
	bloom *BloomFilter[K]

	mut sync.RWMutex
}

// Creates a new LSMTree. Creates ~/.gostore if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](maxSize uint) LSMTree[K, V] {
	// Create application directories
	for _, dir := range appDirs {
		err := mkDir(dir)
		if err != nil {
			log.Fatalf("Error while creating directory %v: %v", dir, err)
		}
	}

	// DATA LAYOUT
	manifest, err := NewManifest[K, V](nil)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.Fatalf("Error loading or creating manifest: %v", err)
	}

	// COMPACTION STRATEGY
	comp := &CompactionImpl[K, V]{}

	// BLOOMFILTER
	var bloom *BloomFilter[K]
	bloom, err = loadBloomFromFile[K](bloomPath)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			bloom = NewBloomFilter[K](BLOOM_SIZE, NUM_HASH_FUNCS)
		} else {
			log.Fatal(err)
		}
	}

	// MEMTABLE
	memtable, err := NewGostoreMemTable[K, V](maxSize)
	if err != nil {
		switch e := err.(type) {
		case *LogApplyErr[K, V]:
			fmt.Println("ERROR WHILE RECREATING DATABASE STATE FROM WRITE AHEAD LOG.")
			fmt.Printf("POSSIBLE DATA LOSS HAS OCCURRED: %v\n", e.Error())
		case *os.PathError:
			// Error opening file. Should have log.Fatal'd on WAL creation if file could not be created
			break
		default:
			log.Fatalf("Error on WAL replay: %v", err)
		}
	}

	return &GoStore[K, V]{memTable: memtable, bloom: bloom, manifest: manifest, compaction: comp}
}

// TODO refactor this
// Write memTable to disk as SSTable
func (self *GoStore[K, V]) flush() {
	snapshot := self.memTable.Snapshot()

	size, err := snapshot.Sync()
	if err != nil {
		panic("Panic on snapshot Sync")
	}
	// Discard memTable & write-ahead log
	self.memTable.Clear()
	self.mut.Unlock()

	// Update manifest
	self.manifest[0].Add(snapshot, size)
	err = self.manifest.Persist(nil)
	if err != nil {
		panic(err)
	}

	// COMPACTION
	for level := range self.manifest[:len(self.manifest)-1] {
		if task := self.compaction.Trigger(level, self.manifest); task != nil {
			fmt.Println("compacting")
			self.compaction.Compact(task, self.manifest)
		}
	}
	return
}

// Write the Key-Value pair to the memtable
func (self *GoStore[K, V]) Write(key K, val V) error {
	self.mut.Lock()

	// Write to memTable
	self.memTable.Put(key, val)
	self.bloom.Add(key)
	if self.memTable.ExceedsSize() {
		go self.flush()
		return nil
	}
	self.mut.Unlock()
	return nil
}

// 1. Check if key exists, exit early if not
// 2. Read from memtable
// 3. Read from level0 (unsorted)
// 4. Read from level 1-3 (sorted)
// Read the value from the given key. Will return error if value is not found.
func (self *GoStore[K, V]) Read(key K) (V, error) {
	self.mut.RLock()
	defer self.mut.RUnlock()
	if !self.bloom.Has(key) {
		return SSTableEntry[K, V]{}.Value, errors.New("Not found")
	}
	// Read from memory
	if val, ok := self.memTable.Get(key); ok {
		return val, nil
	} else {
		level0_tbl := self.manifest[0]

		// Check unsorted level 0
		for _, tbl := range level0_tbl.Tables {
			err := tbl.Load()
			if err != nil {
				return Node[K, V]{}.Value, errors.New("Not found")
			}

			defer tbl.Clear()

			if val, found := tbl.Search(key); found {
				return val, nil
			}
		}

		// binary search sorted levels 1:3
		for _, level := range self.manifest[1:] {
			if val, found := level.BinarySearch(key); found {
				return val, nil
			}
		}
	}
	return Node[K, V]{}.Value, errors.New("Not found")
}

// Delete a key from the DB
func (self *GoStore[K, V]) Delete(key K) error {
	if self.bloom.Has(key) {
		self.memTable.Delete(key)
		self.bloom.Remove(key)
		return nil
	}
	return errors.New("Key not found")
}

// For debugging/tests: Use instead of Close to remove created files and release resources
func (self *GoStore[K, V]) Clean() error {
	err := self.Close()
	if err != nil {
		return nil
	}

	for _, levelDir := range numberToPathMap {
		segments, err := os.ReadDir(levelDir)
		if err != nil {
			return err
		}
		for _, segment := range segments {
			if strings.HasSuffix(segment.Name(), ".segment") {
				err = os.Remove(filepath.Join(levelDir, segment.Name()))
				if err != nil {
					return err
				}
			}
		}
	}
	err = os.Remove(manifestPath)
	if err != nil {
		return err
	}
	err = os.Remove(bloomPath)
	if err != nil {
		return err
	}
	return os.Remove(walPath)
}

// Close closes all associated resources
func (self *GoStore[K, V]) Close() error {
	return self.memTable.Close()
}
