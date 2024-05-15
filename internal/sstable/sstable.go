package sstable

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/pb"
	"google.golang.org/protobuf/proto"
)

// SSTable represents a Sorted String Table. Entries are sorted by key.
type SSTable struct {
	Entries   []*pb.SSTable_Entry // A list of entries sorted by key
	Filter    *filter.BloomFilter // Check if key could be in table
	file      *os.File            // pointer to file descriptor for the table
	Size      int64               // Size of file in bytes
	Name      string              // full filename
	First     []byte              // First key in range
	Last      []byte              // Last key in range
	CreatedOn time.Time           // Timestamp
}

type Opts struct {
	BloomOpts *filter.Opts
	DestDir   string
	Entries   []*pb.SSTable_Entry
}

func New(opts *Opts) *SSTable {
	timestamp := time.Now()
	return &SSTable{
		Name:      filepath.Join(opts.DestDir, GenerateUniqueSegmentName(timestamp)),
		Entries:   opts.Entries,
		Filter:    filter.New(opts.BloomOpts),
		CreatedOn: timestamp,
	}
}

// Test if table key range overlaps the key range of another
func (table *SSTable) Overlaps(anotherTable *SSTable) bool {
	case1 := (slices.Compare(table.First, anotherTable.First) >= 0 && slices.Compare(table.First, anotherTable.Last) <= 0)
	case2 := (slices.Compare(table.Last, anotherTable.First) >= 0 && slices.Compare(table.Last, anotherTable.Last) <= 0)
	return case1 || case2
}

func (table *SSTable) WriteTo(writer io.Writer) (int64, error) {
	b, err := proto.Marshal(&pb.SSTable{Entries: table.Entries})
	if err != nil {
		return -1, err
	}
	byteLength, err := writer.Write(b)
	if err != nil {
		return 0, fmt.Errorf("writer.Write: %w", err)
	}
	return int64(byteLength), nil
}

func (table *SSTable) getFile() (*os.File, error) {
	if table.file != nil {
		return table.file, nil
	}
	var err error
	table.file, err = os.OpenFile(table.Name, os.O_RDWR|os.O_CREATE, 0600)
	return table.file, err
}

// Sync flushes all in-memory entries to stable storage
func (table *SSTable) Sync() (int64, error) {
	fd, err := table.getFile()
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	size, err := table.WriteTo(fd)
	if err != nil {
		return 0, err
	}
	err = fd.Sync()
	if err != nil {
		return 0, err
	}
	table.clearEntries()
	err = table.updateSize(size)
	return table.Size, err
}

func (table *SSTable) updateSize(size int64) error {
	table.Size += size
	return nil
}

func (table *SSTable) clearEntries() {
	table.Entries = []*pb.SSTable_Entry{}
}

func (table *SSTable) SaveFilter() error {
	return table.Filter.Save()
}

func (table *SSTable) LoadFilter() error {
	return table.Filter.Load()
}

// Read entries into memory & locks table
//
// *** You must call Close() after opening table
func (table *SSTable) Open() error {
	if len(table.Entries) > 0 {
		// slog.Warn("Table entries should be empty before calling open")
		return nil
	}
	var err error
	table.file, err = os.Open(table.Name)
	if err != nil {
		return fmt.Errorf("os.OpenFile: %w", err)
	}

	b, err := io.ReadAll(table.file)
	if err != nil {
		return fmt.Errorf("io.ReadAll: %w", err)
	}

	tbl := &pb.SSTable{}
	err = proto.Unmarshal(b, tbl)
	if err != nil {
		return fmt.Errorf("proto.Unmarshal: %w", err)
	}
	table.Entries = tbl.Entries
	return nil
}

// Clears entries, unlocks table, and closes file
//
// Should only be called after prior call to Open()
func (table *SSTable) Close() error {
	table.clearEntries()
	err := table.file.Close()
	if err != nil {
		return fmt.Errorf("file.Close: %w", err)
	}
	return nil
}

// Search searches for a key in the SSTable.
//
// Panics if attempt to search empty entries array
func (table *SSTable) Search(key []byte) ([]byte, bool) {
	assert.True(len(table.Entries) > 0, "Cannot search 0 entries")

	idx, found := sort.Find(len(table.Entries), func(i int) int { return slices.Compare(key, table.Entries[i].Key) })
	if found {
		return table.Entries[idx].Value, true
	}
	return []byte{}, false
}

func (table *SSTable) ToProto() (*pb.SSTable, error) {
	createdOn, err := table.CreatedOn.MarshalBinary()
	if err != nil {
		return nil, err
	}
	p := &pb.SSTable{
		Entries:   []*pb.SSTable_Entry{},
		Name:      &table.Name,
		First:     table.First,
		Last:      table.Last,
		Size:      &table.Size,
		CreatedOn: createdOn,
	}
	if table.Filter == nil {
		return p, nil
	}
	p.Filter = &pb.SSTable_Filter{
		Name: table.Filter.Name,
		Size: table.Filter.Size,
	}
	return p, nil
}
