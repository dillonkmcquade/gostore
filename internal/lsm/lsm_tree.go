package lsm

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

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
	memTable memtable.MemTable[K, V]  // The current memtable
	manifest *manifest.Manifest[K, V] // In-memory representation of on-disk data layout (levels, tables)
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

type dirMaker struct {
	err error
}

func (maker *dirMaker) mkDir(filepath string, perm fs.FileMode) {
	if maker.err != nil {
		return
	}
	maker.err = os.MkdirAll(filepath, perm)
}

// Create the necessary directories
func createAppFiles(opts *LSMOpts) error {
	d := &dirMaker{}
	for _, dir := range opts.ManifestOpts.LevelPaths {
		d.mkDir(dir, 0750)
	}
	d.mkDir(opts.MemTableOpts.FilterOpts.Path, 0750)
	return d.err
}

// Creates a new LSMTree. Creates application directory if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](opts *LSMOpts) (LSM[K, V], error) {
	var errs []error

	// Create application directories
	err := createAppFiles(opts)
	if err != nil {
		errs = append(errs, err)
	}

	// DATA LAYOUT
	manifest, err := manifest.New[K, V](opts.ManifestOpts)
	if err != nil {
		errs = append(errs, err)
	}

	// MEMTABLE
	mem, err := memtable.New[K, V](opts.MemTableOpts)
	if err != nil {
		errs = append(errs, err)
	}

	gostore := &GoStore[K, V]{memTable: mem, manifest: manifest}
	go gostore.waitForFlush()
	return gostore, errors.Join(errs...)
}

func (store *GoStore[K, V]) waitForFlush() {
	for table := range store.memTable.FlushedTables() {
		slog.Debug("Received flushed table, adding to L0")
		store.manifest.AddTable(table, 0)
	}
}

// Write the Key-Value pair to the memtable
func (store *GoStore[K, V]) Write(key K, val V) error {
	err := store.memTable.Put(key, val)
	if err != nil {
		return fmt.Errorf("memTable.Put: %w", err)
	}
	return nil
}

// Read the value from the given key. Will return error if value is not found.
func (store *GoStore[K, V]) Read(key K) (V, error) {
	// Read from memtable first
	if val, ok := store.memTable.Get(key); ok {
		return val, nil
	}

	// Search sstables
	val, err := store.manifest.Search(key)
	if err != nil {
		return ordered.Node[K, V]{}.Value, fmt.Errorf("manifest.Search: %w", err)
	}
	return val, nil
}

// Delete a key from the DB
func (store *GoStore[K, V]) Delete(key K) error {
	store.memTable.Delete(key)
	return nil
}

// Close closes all associated resources
func (store *GoStore[K, V]) Close() error {
	store.memTable.Close()
	return store.manifest.Close()
}
