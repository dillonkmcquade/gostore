package lsm

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/manifest"
	"github.com/dillonkmcquade/gostore/internal/memtable"
	"github.com/dillonkmcquade/gostore/internal/ordered"
)

type LSM[K cmp.Ordered, V any] interface {
	io.Closer
	Write(K, V) error  // Write the Key-Value pair to the memtable
	Read(K) (V, error) // Read the value from the given key.
	Delete(K) error    // Delete the key from the DB
}

type GoStore[K cmp.Ordered, V any] struct {
	// The current memtable
	memTable memtable.MemTable[K, V]

	// Filenames of sstables ordered oldest to most recent
	manifest *manifest.Manifest[K, V]

	mut sync.RWMutex
}

type LSMOpts struct {
	MemTableOpts     *memtable.Opts
	ManifestOpts     *manifest.Opts
	GoStorePath      string
	SSTable_max_size int
}

//	return &LSMOpts{
//		MemTableOpts: &memtable.Opts{
//			WalPath:          filepath.Join(gostorepath, "WAL.log"),
//			Batch_write_size: 10,
//			Max_size:         20000,
//			Bloom_size:       10000,
//			BloomPath:        filepath.Join(gostorepath, "filters"),
//		},
//		ManifestOpts: &manifest.Opts{
//			Path:            filepath.Join(gostorepath, "manifest.log"),
//			Num_levels:      4,
//			Level0_max_size: 300000000,
//			LevelPaths: []string{
//				filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
//				filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
//			},
//			SSTable_max_size: 400000,
//			BloomPath:        filepath.Join(gostorepath, "filters"),
//		},
//		GoStorePath: gostorepath,
//	}
func NewDefaultLSMOpts(gostorepath string) *LSMOpts {
	if gostorepath == "" {
		panic("gostorepath not provided")
	}
	return &LSMOpts{
		MemTableOpts: &memtable.Opts{
			WalPath:          filepath.Join(gostorepath, "WAL.log"),
			Batch_write_size: 10,
			Max_size:         20000,
			FilterOpts: &filter.Opts{
				Size: 400000 * 10,
				Path: filepath.Join(gostorepath, "filters"),
			},
			LevelZero: filepath.Join(gostorepath, "l0"),
		},
		ManifestOpts: &manifest.Opts{
			Path:            filepath.Join(gostorepath, "manifest.log"),
			Num_levels:      4,
			Level0_max_size: 300000000,
			LevelPaths: []string{
				filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
				filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
			},
			SSTable_max_size: 400000,
			BloomPath:        filepath.Join(gostorepath, "filters"),
		},
		GoStorePath: gostorepath,
	}
}

//	return &LSMOpts{
//		MemTableOpts: &memtable.Opts{
//			WalPath:          filepath.Join(gostorepath, "WAL.log"),
//			Batch_write_size: 100,
//			Max_size:         1000,
//			Bloom_size:       10000,
//			BloomPath:        filepath.Join(gostorepath, "filters"),
//		},
//		ManifestOpts: &manifest.Opts{
//			LevelPaths: []string{
//				filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
//				filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
//			},
//			Path:             filepath.Join(gostorepath, "manifest.log"),
//			Num_levels:       4,
//			Level0_max_size:  539375,
//			SSTable_max_size: 1000,
//			BloomPath:        filepath.Join(gostorepath, "filters"),
//		},
//		GoStorePath: gostorepath,
//	}
func NewTestLSMOpts(gostorepath string) *LSMOpts {
	if gostorepath == "" {
		panic("gostorepath not provided")
	}
	return &LSMOpts{
		MemTableOpts: &memtable.Opts{
			Batch_write_size: 100,
			WalPath:          filepath.Join(gostorepath, "WAL.log"),
			Max_size:         1000,
			FilterOpts: &filter.Opts{
				Size: 1000,
				Path: filepath.Join(gostorepath, "filters"),
			},
			LevelZero: filepath.Join(gostorepath, "l0"),
		},
		ManifestOpts: &manifest.Opts{
			LevelPaths: []string{
				filepath.Join(gostorepath, "l0"), filepath.Join(gostorepath, "l1"),
				filepath.Join(gostorepath, "l2"), filepath.Join(gostorepath, "l3"),
			},
			Path:             filepath.Join(gostorepath, "manifest.log"),
			Num_levels:       4,
			Level0_max_size:  539375,
			SSTable_max_size: 1000,
			BloomPath:        filepath.Join(gostorepath, "filters"),
		},
		GoStorePath: gostorepath,
	}
}

// Create the necessary directories
func createAppFiles(opts *LSMOpts) {
	for _, dir := range opts.ManifestOpts.LevelPaths {
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			log.Fatalf("error while creating directory %v: %v", dir, err)
		}
	}
	err := os.MkdirAll(opts.MemTableOpts.FilterOpts.Path, 0750)
	if err != nil {
		log.Fatalf("error while creating directory %v: %v", opts.MemTableOpts.FilterOpts.Path, err)
	}
}

// Creates a new LSMTree. Creates application directory if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](opts *LSMOpts) LSM[K, V] {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic while closing GoStore, recovered")
		}
	}()
	// Create application directories
	createAppFiles(opts)

	// DATA LAYOUT
	manifest, err := manifest.New[K, V](opts.ManifestOpts)
	if err != nil {
		slog.Error("LSM tree: error creating new manifest")
		panic(fmt.Errorf("manifest.New: %w", err))
	}

	// MEMTABLE
	mem, err := memtable.New[K, V](opts.MemTableOpts)
	if err != nil {
		var lae *memtable.LogApplyErr[K, V]
		if errors.As(err, &lae) {
			slog.Error("ERROR WHILE RECREATING DATABASE STATE FROM WRITE AHEAD LOG.")
			slog.Error("POSSIBLE DATA LOSS HAS OCCURRED")
			panic(fmt.Errorf("memtable.New: %w", lae))
		}
		slog.Error("Unknown error occurred while creating memtable")
		panic(fmt.Errorf("memtable.New: %w", err))
	}

	return &GoStore[K, V]{memTable: mem, manifest: manifest}
}

// Write the Key-Value pair to the memtable
func (store *GoStore[K, V]) Write(key K, val V) error {
	err := store.memTable.Put(key, val)
	if err != nil {
		slog.Error("error executing memtable put", "key", key, "value", val)
		slog.Error(err.Error())
		return ErrInternal
	}
	store.mut.Lock()
	defer store.mut.Unlock()
	for _, table := range store.memTable.Purge() {
		store.manifest.AddTable(table, 0)
	}
	store.manifest.Compact()
	return nil
}

// Read the value from the given key. Will return error if value is not found.
func (store *GoStore[K, V]) Read(key K) (V, error) {
	store.mut.Lock()
	defer store.mut.Unlock()

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
				slog.Error("Read: error opening table", "filename", tbl.Name)
				slog.Error(err.Error())
				return ordered.Node[K, V]{}.Value, fmt.Errorf("tbl.Open: %w", err)
			}

			defer func() {
				err := tbl.Close()
				if err != nil {
					slog.Error("error closing table")
				}
			}()

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
					slog.Error("Read: error opening table", "filename", level.Tables[i].Name)
					slog.Error(err.Error())
					return ordered.Node[K, V]{}.Value, fmt.Errorf("tbl.Open: %w", err)
				}
				defer level.Tables[i].Close()
				if val, found := level.Tables[i].Search(key); found {
					return val, nil
				}
			}
		}
	}
	return ordered.Node[K, V]{}.Value, ErrNotFound
}

// Delete a key from the DB
func (store *GoStore[K, V]) Delete(key K) error {
	store.mut.Lock()
	defer store.mut.Unlock()
	store.memTable.Delete(key)
	return nil
}

// Close closes all associated resources
func (store *GoStore[K, V]) Close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	store.memTable.Close()
	return store.manifest.Close()
}
