package sstable

import (
	"cmp"
	"fmt"
	"log/slog"
	"time"

	"github.com/dillonkmcquade/gostore/internal"
	"github.com/dillonkmcquade/gostore/internal/assert"
	"github.com/dillonkmcquade/gostore/internal/ordered"
)

// Merge creates a new SSTable from multiple sorted SSTables
func Merge[K cmp.Ordered, V any](tables ...*SSTable[K, V]) *SSTable[K, V] {
	tree := &ordered.RedBlackTree[K, *Entry[K, V]]{}

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
			if entry.Operation == DELETE {
				tree.Delete(entry.Key)
			} else {
				tree.Put(entry.Key, entry)
			}
		}
	}

	sstable := &SSTable[K, V]{
		Entries: make([]*Entry[K, V], 0, tree.Size()),
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
func Oldest[K cmp.Ordered, V any](tables []*SSTable[K, V]) *SSTable[K, V] {
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
func Split[K cmp.Ordered, V any](table *SSTable[K, V], maxSize int, tableOpts *Opts[K, V]) []*SSTable[K, V] {
	assert.True(len(table.Entries) > maxSize, "Table too small to split")

	var tables []*SSTable[K, V]
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
func Overlapping[K cmp.Ordered, V any](upper_table *SSTable[K, V], lower_level []*SSTable[K, V]) []*SSTable[K, V] {
	if len(lower_level) == 0 {
		return []*SSTable[K, V]{}
	}
	overlaps := []*SSTable[K, V]{}
	for _, lower_table := range lower_level {
		if upper_table.Overlaps(lower_table) {
			overlaps = append(overlaps, lower_table)
		}
	}
	return overlaps
}
