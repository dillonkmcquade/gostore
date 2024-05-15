package lsm

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/manifest"
	"github.com/dillonkmcquade/gostore/internal/memtable"
	"github.com/dillonkmcquade/gostore/internal/ordered"
)

type LSM interface {
	io.Closer
	Write([]byte, []byte) error  // Write the Key-Value pair to the memtable
	Read([]byte) ([]byte, error) // Read the value from the given key.
	Delete([]byte) error         // Delete the key from the DB
}

type GoStore struct {
	memTable memtable.MemTable  // The current memtable
	manifest *manifest.Manifest // In-memory representation of on-disk data layout (levels, tables)
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
			Path:            filepath.Join(gostorepath, "manifest.txtpb"),
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
			Path:             filepath.Join(gostorepath, "manifest.txtpb"),
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
func New(opts *LSMOpts) (LSM, error) {
	var errs []error

	// Create application directories
	err := createAppFiles(opts)
	if err != nil {
		errs = append(errs, err)
	}

	// DATA LAYOUT
	manifest, err := manifest.New(opts.ManifestOpts)
	if err != nil {
		errs = append(errs, err)
	}

	// MEMTABLE
	mem, err := memtable.New(opts.MemTableOpts)
	if err != nil {
		errs = append(errs, err)
	}

	gostore := &GoStore{memTable: mem, manifest: manifest}
	go gostore.waitForFlush()
	return gostore, errors.Join(errs...)
}

func (store *GoStore) waitForFlush() {
	for table := range store.memTable.FlushedTables() {
		slog.Debug("Received flushed table, adding to L0")
		err := store.manifest.AddTable(table, 0)
		if err != nil {
			slog.Error(err.Error())
		}
	}
}

// Write the Key-Value pair to the memtable
func (store *GoStore) Write(key []byte, val []byte) error {
	err := store.memTable.Put(key, val)
	if err != nil {
		return fmt.Errorf("memTable.Put: %w", err)
	}
	return nil
}

// Read the value from the given key. Will return error if value is not found.
func (store *GoStore) Read(key []byte) ([]byte, error) {
	// Read from memtable first
	if val, ok := store.memTable.Get(key); ok {
		return val, nil
	}

	// Search sstables
	val, err := store.manifest.Search(key)
	if err != nil {
		return ordered.Node[[]byte, []byte]{}.Value, fmt.Errorf("manifest.Search: %w", err)
	}
	return val, nil
}

// Delete a key from the DB
func (store *GoStore) Delete(key []byte) error {
	store.memTable.Delete(key)
	return nil
}

// Close closes all associated resources
func (store *GoStore) Close() error {
	err := store.memTable.Close()
	if err != nil {
		slog.Error(err.Error())
	}
	time.Sleep(500 * time.Millisecond)
	err = store.manifest.Close()
	if err != nil {
		slog.Error(err.Error())
	}
	return nil
}
