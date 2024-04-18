package lsm_tree

import (
	"cmp"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	BLOOM_SIZE     = 200 // Size of Bloom filter bitset
	NUM_HASH_FUNCS = 2   // Number of Hash Functions
)

var (
	userHome    = os.Getenv("HOME")
	gostorePath = filepath.Join(userHome, ".gostore")           // Base data directory
	segmentDir  = filepath.Join(gostorePath, "segments")        // Contains all active SSTables
	walPath     = filepath.Join(gostorePath, "wal.dat")         // Path to WAL
	bloomPath   = filepath.Join(gostorePath, "bloomfilter.dat") // Path to saved bloom filter
)

type GoStore[K cmp.Ordered, V any] struct {
	// The current memtable
	memTable MemTable[K, V]

	// Filenames of sstables ordered oldest to most recent
	segments []string

	// Verify if the key exists in the DB quickly
	bloom *BloomFilter[K]

	// The max size before the memtable is flushed to disk
	max_size uint

	// The Write-Ahead-Log (wal) contains a log of all in-memory operations
	// prior to flushing. If the database crashes with data in-memory that has not
	// been written to disk, the current in-memory state may be recreated again after restart.
	wal *WAL[K, V]

	mut sync.RWMutex
}

// Creates a new LSMTree. Creates ~/.gostore if it does not exist.
//
// ***Will exit with non-zero status if error is returned during any of the initialization steps.
func New[K cmp.Ordered, V any](maxSize uint) LSMTree[K, V] {
	// Create ~/.gostore
	_, err := os.Stat(gostorePath)
	if os.IsNotExist(err) {
		err = os.Mkdir(gostorePath, 0777)
		if err != nil {
			log.Fatal(err) // Directory must exist in order to store data files
		}
	}

	// Create ~/.gostore/segments/
	_, err = os.Stat(segmentDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(segmentDir, 0777)
		if err != nil {
			log.Fatal(err) // Directory must exist in order to store data files
		}
	}

	// TREE
	tree := newRedBlackTree[K, V]()

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

	db := &GoStore[K, V]{memTable: tree, bloom: bloom, max_size: maxSize}
	db.wal, err = newWal[K, V](walPath)
	if err != nil {
		log.Fatalf("Failed to create new WAL: %s", err)
	}

	// Recreate previous state if a wal.dat exists
	err = db.Replay(walPath)
	if err != nil && err != io.EOF {
		switch e := err.(type) {
		case *LogApplyErr[K, V]:
			fmt.Println("ERROR WHILE RECREATING DATABASE STATE FROM WRITE AHEAD LOG.")
			fmt.Printf("POSSIBLE DATA LOSS HAS OCCURRED: %v\n", e.Error())
		case *os.PathError:
			goto end
		default:
			log.Fatalf("Error on WAL replay: %v", err)
		}
	}
	// Create new WAL
end:
	return db
}

// Iterate over segments from newest to oldest
type SSTableIterator struct {
	index    int
	segments []string
}

func (iter *SSTableIterator) HasNext() bool {
	return iter.index > 0
}

func (iter *SSTableIterator) Next() string {
	if iter.HasNext() {
		iter.index--
		segment := iter.segments[iter.index]
		return segment
	}
	return ""
}

// Returns a newest -> oldest segment iterator
func newSSTableIterator(segments *[]string) *SSTableIterator {
	return &SSTableIterator{index: len(*segments), segments: *segments}
}

// Write memTable to disk as SSTable
func (self *GoStore[K, V]) flush() {
	// Persist in-memory data
	table := filepath.Join(segmentDir, fmt.Sprintf("%v.segment", time.Now().Unix()))
	err := writeSSTable(self.memTable, table)
	if err != nil {
		log.Fatalf("Unable to build SSTable : %v", err)
	}
	self.mut.Unlock()

	// Save filename for reads
	self.segments = append(self.segments, table)

	// Discard memTable
	self.memTable.Clear()

	// Discard write-ahead log
	self.wal.Discard()
}

func (self *GoStore[L, V]) exceeds_size() bool {
	return self.memTable.Size() > self.max_size
}

// Insert to memtable
func (self *GoStore[K, V]) Write(key K, val V) error {
	self.mut.Lock()

	// Write to memTable
	self.memTable.Put(key, val)
	err := self.wal.Write(key, val)
	if err != nil {
		return err
	}
	self.bloom.Add(key)
	if self.exceeds_size() {
		go self.flush()
		return nil
	}
	self.mut.Unlock()
	return nil
}

func (self *GoStore[K, V]) Read(key K) (V, error) {
	self.mut.RLock()
	// Read from memory
	if val, ok := self.memTable.Get(key); ok {
		return val, nil
	} else {
		// Read from disk
		iter := newSSTableIterator(&self.segments)
		for iter.HasNext() {
			filename := iter.Next()
			table, err := readSSTable[K, V](filename)
			if err != nil {
				return Node[K, V]{}.Value, err
			}
			if val, ok := table.Search(key); ok {
				return val, nil
			} else {
				continue
			}

		}
	}
	self.mut.RUnlock()
	return Node[K, V]{}.Value, nil
}

func (self *GoStore[K, V]) Delete(key K) error {
	panic("Unimplemented")
}

// Replay replays the Write-Ahead Log and applies changes to the database.
func (self *GoStore[K, V]) Replay(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	for {
		entry := &LogEntry[K, V]{}
		if err = dec.Decode(entry); err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err)
			break // End of log file
		}

		// Apply the entry to the database
		switch entry.Operation {
		case INSERT:
			self.memTable.Put(entry.Key, entry.Value)
		case DELETE:
			panic("Unimplemented")
		}
	}
	return err
}

func (self *GoStore[K, V]) Clean() error {
	err := self.Close()
	if err != nil {
		return nil
	}
	return os.Remove(walPath)
}

// Close closes all associated resources
func (self *GoStore[K, V]) Close() error {
	return self.wal.Close()
}
