package lsm_tree

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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

	levelPaths []string

	mut sync.RWMutex
}

type LSMOpts struct {
	BloomOpts    *BloomFilterOpts
	MemTableOpts *GoStoreMemTableOpts
	ManifestOpts *ManifestOpts
	GoStorePath  string
	BloomPath    string
	LevelPaths   []string
}

func NewDefaultLSMOpts(gostorepath string) *LSMOpts {
	return &LSMOpts{
		BloomOpts: &BloomFilterOpts{
			size:         960000,
			numHashFuncs: 7,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  filepath.Join(gostorepath, "wal.dat"),
			max_size: 20000,
		},
		ManifestOpts: &ManifestOpts{
			Path:            filepath.Join(gostorepath, "manifest.json"),
			Num_levels:      4,
			Level0_max_size: 300,
		},
		GoStorePath: gostorepath,
		BloomPath:   filepath.Join(gostorepath, "bloom.dat"),
		LevelPaths: []string{
			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
		},
	}
}

// Smaller defaults used for testing
func NewTestLSMOpts(gostorepath string) *LSMOpts {
	return &LSMOpts{
		BloomOpts: &BloomFilterOpts{
			size:         1000,
			numHashFuncs: 1,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  filepath.Join(gostorepath, "wal.dat"),
			max_size: 1000,
		},
		ManifestOpts: &ManifestOpts{
			Path:            filepath.Join(gostorepath, "manifest.json"),
			Num_levels:      4,
			Level0_max_size: 1,
		},
		GoStorePath: gostorepath,
		BloomPath:   filepath.Join(gostorepath, "bloom.dat"),
		LevelPaths: []string{
			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
		},
	}
}

func createAppFiles(opts *LSMOpts) {
	for _, dir := range opts.LevelPaths {
		err := mkDir(dir)
		if err != nil {
			log.Fatalf("Error while creating directory %v: %v", dir, err)
		}
	}
}

// Creates a new LSMTree. Creates ~/.gostore if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](opts *LSMOpts) LSMTree[K, V] {
	// Create application directories
	createAppFiles(opts)

	// DATA LAYOUT
	manifest, err := NewManifest[K, V](opts.ManifestOpts)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.Fatalf("Error loading or creating manifest: %v", err)
	}

	// COMPACTION STRATEGY
	comp := &CompactionImpl[K, V]{LevelPaths: opts.LevelPaths}

	// BLOOMFILTER
	var bloom *BloomFilter[K]
	bloom, err = loadBloomFromFile[K](opts.BloomPath)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			bloom = NewBloomFilter[K](opts.BloomOpts)
		} else {
			log.Fatal(err)
		}
	}

	// MEMTABLE
	memtable, err := NewGostoreMemTable[K, V](opts.MemTableOpts)
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

	return &GoStore[K, V]{memTable: memtable, bloom: bloom, manifest: manifest, compaction: comp, levelPaths: opts.LevelPaths}
}

// Write memTable to disk as SSTable
func (self *GoStore[K, V]) flush() {
	snapshot := self.memTable.Snapshot(self.levelPaths[0])

	size, err := snapshot.Sync()
	if err != nil {
		panic("Panic on snapshot Sync")
	}
	// Discard memTable & write-ahead log
	self.memTable.Clear()
	self.mut.Unlock()

	// Update manifest
	self.manifest.Levels[0].Add(snapshot, size)
	err = self.manifest.Persist()
	if err != nil {
		panic(err)
	}

	// COMPACTION
	for level := range self.manifest.Levels[:len(self.manifest.Levels)-1] {
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
	if !self.bloom.Has(key) {
		return SSTableEntry[K, V]{}.Value, errors.New("Not found")
	}
	self.mut.RLock()
	defer self.mut.RUnlock()

	// Read from memory
	if val, ok := self.memTable.Get(key); ok {
		return val, nil
	} else {
		level0_tbl := self.manifest.Levels[0]

		// Check unsorted level 0
		// TODO search newest files first
		for _, tbl := range level0_tbl.Tables {
			err := tbl.Open()
			if err != nil {
				return Node[K, V]{}.Value, errors.New("Not found")
			}

			defer tbl.Close()

			if val, found := tbl.Search(key); found {
				return val, nil
			}
		}

		// binary search sorted levels 1:3
		for _, level := range self.manifest.Levels[1:] {
			if i, found := level.BinarySearch(key); found {
				val, found := level.Tables[i].Search(key)
				if found {
					return val, nil
				}
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

// Close closes all associated resources
func (self *GoStore[K, V]) Close() error {
	return self.memTable.Close()
}
