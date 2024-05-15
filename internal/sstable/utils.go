package sstable

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dillonkmcquade/gostore/internal"
	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/filter"
	"github.com/dillonkmcquade/gostore/internal/ordered"
	"github.com/dillonkmcquade/gostore/internal/pb"
)

// Return sorted output stream of SSTable_Entry from an arbitrary number of tables
func Merge(tables ...*SSTable) <-chan *pb.SSTable_Entry {
	tree := ordered.Rbt[[]byte, *pb.SSTable_Entry](slices.Compare[[]byte])
	for _, table := range tables {
		if len(table.Entries) == 0 {
			err := table.Open()
			defer table.Close()
			if err != nil {
				slog.Error("merge: error opening table", "filename", table.Name)
				panic(err)
			}
		}
		assert.True(len(table.Entries) > 0, "Expected table with entries, found %v entries", len(table.Entries))

		for _, entry := range table.Entries {
			tree.Put(entry.Key, entry)
		}
	}

	return tree.Values()
}

// Find and return the oldest table
func Oldest(tables []*SSTable) *SSTable {
	// Tables should never be empty if it triggered compaction
	assert.True(len(tables) > 0, "Cannot find oldest table from slice of length 0")

	oldest := tables[0]

	for i := 1; i < len(tables); i++ {
		if tables[i].CreatedOn.Before(oldest.CreatedOn) {
			oldest = tables[i]
		}
	}
	return oldest
}

// Form SSTables of maxSize from input stream
func Split(in <-chan *pb.SSTable_Entry, maxSize int, tableOpts *Opts) <-chan *SSTable {
	ch := make(chan *SSTable)

	go func() {
		tbl := New(tableOpts)
		defer close(ch)
		for entry := range in {
			tbl.Entries = append(tbl.Entries, entry)
			if len(tbl.Entries) == 1 {
				tbl.First = entry.Key
			}

			tbl.Filter.Add(entry.Key)

			if len(tbl.Entries) >= maxSize {
				tbl.Last = tbl.Entries[len(tbl.Entries)-1].Key
				ch <- tbl
				tbl = New(tableOpts)
			}
		}
		if len(tbl.Entries) > 0 {
			tbl.Last = tbl.Entries[len(tbl.Entries)-1].Key
			ch <- tbl
		}
	}()

	return ch
}

// Generate a unique SSTable filename in the format TIMESTAMP_UNIQUESTRING.segment
func GenerateUniqueSegmentName(time time.Time) string {
	uniqueString, err := internal.GenerateRandomString(8)
	if err != nil {
		slog.Error("error occurred while generating random string")
		panic(err)
	}
	return fmt.Sprintf("%v_%v.segment", time.Unix(), uniqueString)
}

// Return tables from lower_level that overlap upper_table
func Overlapping(upper_table *SSTable, lower_level []*SSTable) []*SSTable {
	if len(lower_level) == 0 {
		return []*SSTable{}
	}
	overlaps := []*SSTable{}
	for _, lower_table := range lower_level {
		if upper_table.Overlaps(lower_table) {
			overlaps = append(overlaps, lower_table)
		}
	}
	return overlaps
}

func FromProto(p *pb.SSTable) (*SSTable, error) {
	var tm time.Time
	err := tm.UnmarshalBinary(p.GetCreatedOn())
	if err != nil {
		return nil, err
	}
	t := &SSTable{
		Entries: p.GetEntries(),
		Filter: &filter.BloomFilter{
			Name: p.GetFilter().GetName(),
			Size: p.GetFilter().GetSize(),
		},
		Size:  p.GetSize(),
		Name:  p.GetName(),
		First: p.GetFirst(),
		Last:  p.GetLast(),
	}
	return t, nil
}
