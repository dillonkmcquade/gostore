package lsm_tree

import (
	"cmp"
	"io"
	"log"
	"log/slog"
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

	// Paths to the level directories. Each index corresponds to a level.
	levelPaths []string

	mut sync.RWMutex
}

type LSMOpts struct {
	BloomOpts        *BloomFilterOpts
	MemTableOpts     *GoStoreMemTableOpts
	ManifestOpts     *ManifestOpts
	GoStorePath      string
	BloomPath        string
	LevelPaths       []string
	SSTable_max_size int
}

//	return &LSMOpts{
//		BloomOpts: &BloomFilterOpts{
//			size:         960000,
//			numHashFuncs: 7,
//		},
//		MemTableOpts: &GoStoreMemTableOpts{
//			walPath:  filepath.Join(gostorepath, "wal.dat"),
//			max_size: 20000,
//		},
//		ManifestOpts: &ManifestOpts{
//			Path:            filepath.Join(gostorepath, "manifest.json"),
//			Num_levels:      4,
//			Level0_max_size: 300,
//		},
//		GoStorePath: gostorepath,
//		BloomPath:   filepath.Join(gostorepath, "bloom.dat"),
//		LevelPaths: []string{
//			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
//			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
//		},
//	}
func NewDefaultLSMOpts(gostorepath string) *LSMOpts {
	return &LSMOpts{
		BloomOpts: &BloomFilterOpts{
			Size:         960000,
			NumHashFuncs: 7,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  filepath.Join(gostorepath, generateUniqueWALName()),
			Max_size: 20000,
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
		SSTable_max_size: 40_000_000,
	}
}

// Smaller defaults used for testing
//
//	return &LSMOpts{
//		BloomOpts: &BloomFilterOpts{
//			size:         1000,
//			numHashFuncs: 1,
//		},
//		MemTableOpts: &GoStoreMemTableOpts{
//			walPath:  filepath.Join(gostorepath, "wal.dat"),
//			max_size: 1000,
//		},
//		ManifestOpts: &ManifestOpts{
//			Path:            filepath.Join(gostorepath, "manifest.json"),
//			Num_levels:      4,
//			Level0_max_size: 1,
//		},
//		GoStorePath: gostorepath,
//		BloomPath:   filepath.Join(gostorepath, "bloom.dat"),
//		LevelPaths: []string{
//			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
//			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
//		},
//	}
func NewTestLSMOpts(gostorepath string) *LSMOpts {
	return &LSMOpts{
		BloomOpts: &BloomFilterOpts{
			Size:         100000,
			NumHashFuncs: 3,
		},
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:  filepath.Join(gostorepath, generateUniqueWALName()),
			Max_size: 1000,
		},
		ManifestOpts: &ManifestOpts{
			Path:            filepath.Join(gostorepath, "manifest.json"),
			Num_levels:      4,
			Level0_max_size: 539375,
		},
		GoStorePath: gostorepath,
		BloomPath:   filepath.Join(gostorepath, "bloom.dat"),
		LevelPaths: []string{
			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
		},
		SSTable_max_size: 1000,
	}
}

func createAppFiles(opts *LSMOpts) {
	for _, dir := range opts.LevelPaths {
		err := mkDir(dir)
		if err != nil {
			log.Fatalf("error while creating directory %v: %v", dir, err)
		}
	}
}

// Creates a new LSMTree. Creates application directory if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](opts *LSMOpts) LSMTree[K, V] {
	// Create application directories
	createAppFiles(opts)

	// DATA LAYOUT
	manifest, err := NewManifest[K, V](opts.ManifestOpts)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.Fatalf("error loading or creating manifest: %v", err)
	}

	// COMPACTION STRATEGY
	comp := &CompactionImpl[K, V]{LevelPaths: opts.LevelPaths, SSTable_max_size: opts.SSTable_max_size}

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
			slog.Error("ERROR WHILE RECREATING DATABASE STATE FROM WRITE AHEAD LOG.")
			slog.Error("POSSIBLE DATA LOSS HAS OCCURRED")
			slog.Error(e.Error())
		case *os.PathError:
			// Error opening file. Should have log.Fatal'd on WAL creation if file could not be created
			break
		default:
			log.Fatalf("error on WAL replay: %v", err)
		}
	}

	return &GoStore[K, V]{memTable: memtable, bloom: bloom, manifest: manifest, compaction: comp, levelPaths: opts.LevelPaths}
}

// Write memTable to disk as SSTable
func (self *GoStore[K, V]) flush() {
	// create sstable
	snapshot := self.memTable.Snapshot(self.levelPaths[0])
	slog.Debug("Flush", "size", len(snapshot.Entries), "filename", snapshot.Name)

	// save to file
	_, err := snapshot.Sync()
	if err != nil {
		panic("panic on snapshot Sync")
	}

	// Discard memTable & write-ahead log
	self.memTable.Clear()
	self.mut.Unlock()

	self.manifest.Levels[0].Add(snapshot)
	// err = self.manifest.Persist()
	// if err != nil {
	// 	panic(err)
	// }

	self.compaction.Compact(self.manifest)
}

// Write the Key-Value pair to the memtable
func (self *GoStore[K, V]) Write(key K, val V) error {
	// Write to memTable
	self.mut.Lock()
	self.memTable.Put(key, val)
	self.bloom.Add(key)
	if self.memTable.ExceedsSize() {
		self.flush()
		return nil
	}
	self.mut.Unlock()
	return nil
}

// Read the value from the given key. Will return error if value is not found.
func (self *GoStore[K, V]) Read(key K) (V, error) {
	self.mut.RLock()
	defer self.mut.RUnlock()
	if !self.bloom.Has(key) {
		return SSTableEntry[K, V]{}.Value, ErrNotFound
	}

	// Read from memory
	if val, ok := self.memTable.Get(key); ok {
		return val, nil
	}

	level0 := self.manifest.Levels[0]

	// Check unsorted level 0
	for i := len(level0.Tables) - 1; i >= 0; i-- {
		tbl := level0.Tables[i]
		err := tbl.Open()
		if err != nil {
			slog.Error("File I/O", "cause", err)
			return Node[K, V]{}.Value, FileIOErr
		}
		defer tbl.Close()

		if val, found := tbl.Search(key); found {
			return val, nil
		}

	}

	// binary search sorted levels 1:3
	for _, level := range self.manifest.Levels[1:] {
		slog.Debug("Reading", "level", level.Number, "key", key, "#tables", len(level.Tables))
		if i, found := level.BinarySearch(key); found {
			err := level.Tables[i].Open()
			if err != nil {
				slog.Error("File I/O", "error", err)
				panic(err)
			}
			if val, found := level.Tables[i].Search(key); found {
				return val, nil
			}
			defer level.Tables[i].Close()
		}
	}
	return Node[K, V]{}.Value, ErrNotFound
}

// Delete a key from the DB
func (self *GoStore[K, V]) Delete(key K) error {
	if self.bloom.Has(key) {
		self.memTable.Delete(key)
		self.bloom.Remove(key)
		return nil
	}
	return ErrNotFound
}

// Close closes all associated resources
func (self *GoStore[K, V]) Close() error {
	return self.memTable.Close()
}
