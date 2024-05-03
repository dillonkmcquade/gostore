package lsm_tree

import (
	"cmp"
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

	// Paths to the level directories. Each index corresponds to a level.
	levelPaths []string

	mut sync.RWMutex
}

type LSMOpts struct {
	MemTableOpts     *GoStoreMemTableOpts
	ManifestOpts     *ManifestOpts
	GoStorePath      string
	LevelPaths       []string
	SSTable_max_size int
}

//	return &LSMOpts{
//		MemTableOpts: &GoStoreMemTableOpts{
//			walPath:          filepath.Join(gostorepath, generateUniqueWALName()),
//			Batch_write_size: 10,
//			Max_size:         20000,
//			Bloom_size:       10000,
//			BloomPath:        filepath.Join(gostorepath, "filters"),
//		},
//		ManifestOpts: &ManifestOpts{
//			Path:            filepath.Join(gostorepath, "manifest.dat"),
//			Num_levels:      4,
//			Level0_max_size: 300,
//		},
//		GoStorePath: gostorepath,
//		LevelPaths: []string{
//			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
//			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
//		},
//		SSTable_max_size: 40_000_000,
//	}
func NewDefaultLSMOpts(gostorepath string) *LSMOpts {
	return &LSMOpts{
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:          filepath.Join(gostorepath, "WAL.log"),
			Batch_write_size: 10,
			Max_size:         20000,
			Bloom_size:       10000,
			BloomPath:        filepath.Join(gostorepath, "filters"),
		},
		ManifestOpts: &ManifestOpts{
			Path:            filepath.Join(gostorepath, "manifest.log"),
			Num_levels:      4,
			Level0_max_size: 300,
		},
		GoStorePath: gostorepath,
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
//		MemTableOpts: &GoStoreMemTableOpts{
//			walPath:          filepath.Join(gostorepath, generateUniqueWALName()),
//			Batch_write_size: 100,
//			Max_size:         1000,
//			Bloom_size:       10000,
//			BloomPath:        filepath.Join(gostorepath, "filters"),
//		},
//		ManifestOpts: &ManifestOpts{
//			Path:            filepath.Join(gostorepath, "manifest.dat"),
//			Num_levels:      4,
//			Level0_max_size: 539375,
//		},
//		GoStorePath: gostorepath,
//		LevelPaths: []string{
//			filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
//			filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
//		},
//		SSTable_max_size: 1000,
//	}
func NewTestLSMOpts(gostorepath string) *LSMOpts {
	return &LSMOpts{
		MemTableOpts: &GoStoreMemTableOpts{
			walPath:          filepath.Join(gostorepath, "WAL.log"),
			Batch_write_size: 100,
			Max_size:         1000,
			Bloom_size:       10000,
			BloomPath:        filepath.Join(gostorepath, "filters"),
		},
		ManifestOpts: &ManifestOpts{
			Path:            filepath.Join(gostorepath, "manifest.log"),
			Num_levels:      4,
			Level0_max_size: 539375,
		},
		GoStorePath: gostorepath,
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
	err := mkDir(opts.MemTableOpts.BloomPath)
	if err != nil {
		log.Fatalf("error while creating directory %v: %v", opts.MemTableOpts.BloomPath, err)
	}
}

// Creates a new LSMTree. Creates application directory if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](opts *LSMOpts) LSMTree[K, V] {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic while closing GoStore, recovered")
		}
	}()
	// Create application directories
	createAppFiles(opts)

	// DATA LAYOUT
	manifest, err := NewManifest[K, V](opts.ManifestOpts)
	if err != nil {
		log.Fatal(err)
	}

	// COMPACTION STRATEGY
	comp := &CompactionImpl[K, V]{LevelPaths: opts.LevelPaths, SSTable_max_size: opts.SSTable_max_size, BloomPath: opts.MemTableOpts.BloomPath}

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
			logError(e)
		default:
			logError(err)
			os.Exit(1)
		}
	}

	return &GoStore[K, V]{memTable: memtable, manifest: manifest, compaction: comp, levelPaths: opts.LevelPaths}
}

// Write memTable to disk as SSTable
func (store *GoStore[K, V]) flush() {
	// create sstable
	snapshot := store.memTable.Snapshot(store.levelPaths[0])
	slog.Debug("Flush", "size", len(snapshot.Entries), "filename", snapshot.Name)

	// save to file
	_, err := snapshot.Sync()
	if err != nil {
		panic(err)
	}
	err = snapshot.SaveFilter()
	if err != nil {
		panic(err)
	}

	// Discard memTable & write-ahead log
	store.memTable.Clear()
	store.mut.Unlock()
	store.manifest.AddTable(snapshot, 0)
	store.compaction.Compact(store.manifest)
}

// Write the Key-Value pair to the memtable
func (store *GoStore[K, V]) Write(key K, val V) error {
	store.mut.Lock()
	store.memTable.Put(key, val)
	if store.memTable.ExceedsSize() {
		store.flush() // flush unlocks mutex
		return nil
	}
	store.mut.Unlock()
	return nil
}

// Read the value from the given key. Will return error if value is not found.
func (store *GoStore[K, V]) Read(key K) (V, error) {
	store.mut.RLock()
	defer store.mut.RUnlock()

	// Read from memtable first
	if val, ok := store.memTable.Get(key); ok {
		return val, nil
	}

	level0 := store.manifest.Levels[0]

	// Check unsorted level 0
	// Last index in level0 tables is the most recent, so we read in descending order
	for i := len(level0.Tables) - 1; i >= 0; i-- {
		tbl := level0.Tables[i]

		if tbl.Filter.Has(key) {
			err := tbl.Open()
			if err != nil {
				logError(err)
				return Node[K, V]{}.Value, ErrFileIO
			}
			defer tbl.Close()
			if val, found := tbl.Search(key); found {
				return val, nil
			}
		}
	}

	// binary search sorted levels 1:3 sequentially
	for _, level := range store.manifest.Levels[1:] {
		slog.Debug("Reading", "level", level.Number, "key", key, "#tables", len(level.Tables))
		if i, found := level.BinarySearch(key); found {
			if level.Tables[i].Filter.Has(key) {
				err := level.Tables[i].Open()
				if err != nil {
					logError(err)
					panic(err)
				}
				if val, found := level.Tables[i].Search(key); found {
					return val, nil
				}
				defer level.Tables[i].Close()
			}
		}
	}
	return Node[K, V]{}.Value, ErrNotFound
}

// Delete a key from the DB
func (store *GoStore[K, V]) Delete(key K) error {
	store.memTable.Delete(key)
	return nil
}

// Close closes all associated resources
func (store *GoStore[K, V]) Close() error {
	return store.manifest.Close()
}
