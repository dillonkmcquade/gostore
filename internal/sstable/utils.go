package sstable

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dillonkmcquade/gostore/internal"
	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/ordered"
	"github.com/dillonkmcquade/gostore/internal/pb"
)

// Merge creates a new SSTable from multiple sorted SSTables
func Merge(tables ...*SSTable) *SSTable {
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

	sstable := &SSTable{
		Entries: make([]*pb.SSTable_Entry, 0, tree.Size()),
	}

	iter := tree.Values()
	for iter.HasNext() {
		node := iter.Next()
		sstable.Entries = append(sstable.Entries, node)
	}
	sstable.First = sstable.Entries[0].Key
	sstable.Last = sstable.Entries[len(sstable.Entries)-1].Key
	return sstable
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

// Split a table into multiple tables by size
func Split(table *SSTable, maxSize int, tableOpts *Opts) []*SSTable {
	assert.True(len(table.Entries) > maxSize, "Table too small to split")

	var tables []*SSTable
	offset := maxSize

	var i int
	for i = 0; i < len(table.Entries); {
		var lastIndex int
		if i+int(offset)-1 < 0 {
			lastIndex = 0
		} else {
			lastIndex = min(i+int(offset)-1, len(table.Entries)-1)
		}

		tbl := New(tableOpts)
		tbl.Entries = table.Entries[i : lastIndex+1]
		tbl.First = table.Entries[i].Key
		tbl.Last = table.Entries[lastIndex].Key
		for _, e := range table.Entries[i : lastIndex+1] {
			tbl.Filter.Add(e.Key)
		}
		tables = append(tables, tbl)

		i += int(offset)

	}
	return tables
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
